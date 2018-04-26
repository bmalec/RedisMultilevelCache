using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.Caching;
using StackExchange.Redis;


namespace RedisMultilevelCache
{
    public class MultilevelCacheProvider
    {
    private const int HASH_SLOT_COUNT = 16384;
    private const string SYNC_CHANNEL_NAME = "RedisMultilevelCache_Sync";

    private class LocalCacheEntry<T>
    {
      /// <summary>
      /// Hashslot of this key this value is stored under
      /// </summary>
      public ushort KeyHashSlot { get; private set; }
      /// <summary>
      /// Time this entry was written to the cache
      /// </summary>
      public long Timestamp { get; private set; }
      /// <summary>
      /// Actual data value associated with this key
      /// </summary>
      public T Data { get; private set; }

      /// <summary>
      /// Public constructor
      /// </summary>
      /// <param name="keyHashSlot">Hashslot of the key associated with this entry</param>
      /// <param name="timestamp">Time the entry was written to the cache</param>
      /// <param name="data">Actual data to associated with the key</param>
      public LocalCacheEntry(ushort keyHashSlot, long timestamp, T data)
      {
        KeyHashSlot = keyHashSlot;
        Timestamp = timestamp;
        Data = data;
      }
    }

    /// <summary>
    /// This GUID is used to uniqely identify the cache instance
    /// </summary>
    private readonly Guid _instanceId = Guid.NewGuid();
    /// <summary>
    /// Serialization provider to encode/decode byte arrays sent to Redis
    /// </summary>
    private readonly ISerializationProvider _serializationProvider;

    /// <summary>
    /// This array stores the timestamp that a key with the corresponding hash slot was last updated
    /// </summary>
    /// <remarks>
    /// .Net will initialize all items to zero, which is a fine default for us
    /// </remarks>
    private readonly long[] _lastUpdated = new long[HASH_SLOT_COUNT];  // the array element index is the hash slot; for hash slot 3142 look up _lastUpdatedDictionary[3142]

    /// <summary>
    /// StackExchange.Redis connection to the Redis server
    /// </summary>
    private readonly ConnectionMultiplexer _redisConnection;

    /// <summary>
    /// StackExchange.Redis database 
    /// </summary>
    private readonly IDatabase _redisDb;

    /// <summary>
    /// In-process cache used as the L1 cache
    /// </summary>
    private readonly MemoryCache _inProcessCache = MemoryCache.Default;


    /// <summary>
    /// Public constructor
    /// </summary>
    /// <param name="redisConnectionString">StackExchange.Redis connection string</param>
    public MultilevelCacheProvider(string redisConnectionString) : this(redisConnectionString, new DefaultSerializationProvider())
    {
    }


    /// <summary>
    /// Public constructor
    /// </summary>
    /// <param name="redisConnectionString">StackExchange.Redis connection string</param>
    /// <param name="serializationProvider">Alternate serialization provider</param>
    public MultilevelCacheProvider(string redisConnectionString, ISerializationProvider serializationProvider)
    {
      _serializationProvider = serializationProvider ?? throw new ArgumentNullException(nameof(serializationProvider));

      _redisConnection = ConnectionMultiplexer.Connect(redisConnectionString);
      _redisConnection.PreserveAsyncOrder = false; // This improves performance since message order isn't important for us; see https://stackexchange.github.io/StackExchange.Redis/PubSubOrder.html
      _redisDb = _redisConnection.GetDatabase();

      // wire up Redis pub/sub for cache synchronization messages:

      _redisConnection.GetSubscriber().Subscribe(SYNC_CHANNEL_NAME, DataSynchronizationMessageHandler);
    }


    /// <summary>
    /// Message handler for hash key invalidation messages
    /// </summary>
    /// <param name="channel">Redis pub/sub channel name</param>
    /// <param name="message">Pub/sub message</param>
    private void DataSynchronizationMessageHandler(RedisChannel channel, RedisValue message)
    {
      // Early out, if channel name doesn't match our sync channel

      if (string.Compare(channel, SYNC_CHANNEL_NAME, StringComparison.InvariantCultureIgnoreCase) != 0)
      {
        return;
      }

      // otherwise, deserialize the message

      var dataSyncMessage = DataSyncMessage.Deserialize(message);

      // and update the appropriate _lastUpdated element with the current timestamp
      // Invalidate.Exchange() would make more sense here, but the lock statement
      // makes the purpose more evident for an example

      lock (_lastUpdated)
      {
        _lastUpdated[dataSyncMessage.KeyHashSlot] = Stopwatch.GetTimestamp();
      }
    }


    /// <summary>
    /// Add/update an entry in the cache
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="key">Key used to locate value in the cache</param>
    /// <param name="value"></param>
    /// <param name="ttl"></param>
    public void Set<T>(string key, T value, TimeSpan ttl)
    {
      // parameter validation

      if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));
      if (value == null) throw new ArgumentNullException(nameof(value));
      if (ttl.TotalSeconds < 1) throw new ArgumentNullException(nameof(ttl));

      // Get the current timestamp before we do anything else.  Decrement it by one so any sync messages processed during 
      // this method call will force an immediate expiration of the key's hash slot

      var timestamp = Stopwatch.GetTimestamp() - 1;

      var keyHashSlot = HashSlotCalculator.CalculateHashSlot(key);

      byte[] serializedData = null;

      // Serialize the data and write to Redis

      using (MemoryStream ms = new MemoryStream())
      {
        _serializationProvider.Serialize<T>(ms, value);
        serializedData = ms.ToArray();
      }

      // Execute the Redis SET and PUBLISH operations in one round trip using Lua
      // (Could use StackExchange.Redis batch here, instead)

      string luaScript = @"
        redis.call('SET', KEYS[1], ARGV[1], 'EX',  ARGV[2])
        redis.call('PUBLISH', ARGV[3], ARGV[4])
      ";

      var scriptArgs = new RedisValue[4];
      scriptArgs[0] = serializedData;
      scriptArgs[1] = ttl.TotalSeconds;
      scriptArgs[2] = SYNC_CHANNEL_NAME;
      scriptArgs[3] = DataSyncMessage.Create(_instanceId, keyHashSlot).Serialize();

      _redisDb.ScriptEvaluate(luaScript, new RedisKey[] { key }, scriptArgs);

      // Update the in-process cache

      _inProcessCache.Set(key, new LocalCacheEntry<T>(keyHashSlot, timestamp, value), DateTimeOffset.UtcNow.Add(ttl));
    }


    /// <summary>
    /// Retrieve an item from the cache
    /// </summary>
    /// <typeparam name="T">Object type to return</typeparam>
    /// <param name="key">Key</param>
    /// <returns>Object of type T associated with the key</returns>
    public T Get<T>(string key)
    {
      // Get the current timestamp before we do anything else.  Decrement it by one so any sync messages processed during 
      // this method call will force an immediate expiration of the key's hash slot

      var timestamp = Stopwatch.GetTimestamp() - 1;

      int keyHashSlot = -1;  // -1 is used as a sentinel value used to determine if the key's hash slot has already been computed

      // attempt to retreive value from the in-process cache

      var inProcessCacheEntry = _inProcessCache.Get(key) as LocalCacheEntry<T>;

      if (inProcessCacheEntry != null)
      {
        // found the entry in the in-process cache, now
        // need to check if the entry may be stale and we
        // need to re-read from Redis

        keyHashSlot = inProcessCacheEntry.KeyHashSlot;

        // Check the timestamp of the cache entry versus the _lastUpdated array
        // If the timestamp of the cache entry is greater than what's in _lastUpdated, 
        // then we can just return the value from the in-process cache

        // Could use and Interlocked operation here, but lock is a more obvious in intent

        lock (_lastUpdated)
        {
          if (_lastUpdated[keyHashSlot] < inProcessCacheEntry.Timestamp)
          {
            return (T) inProcessCacheEntry.Data;
          }
        }
      }

      // if we've made it to here, the key is either not in the in-process cache or 
      // the in-process cache entry is stale.  In either case we need to hit Redis to 
      // get the correct value, and the key's remaining TTL in Redis

      T value = default(T);

      string luaScript = @"
        local result={}
        result[1] = redis.call('GET', KEYS[1])
        result[2] = redis.call('TTL', KEYS[1])
        return result;
      ";

      RedisValue[] results = (RedisValue[]) _redisDb.ScriptEvaluate(luaScript, new RedisKey[] { key });

      if (!results[0].IsNull)
      {
        var serializedData = (byte[])results[0];

        if (serializedData.Length > 0)
        {
          // Deserialize the bytes returned from Redis

          using (MemoryStream ms = new MemoryStream(serializedData))
          {
            value = _serializationProvider.Deserialize<T>(ms);
          }

          // Don't want to have to recalculate the hashslot twice, so test if it's already
          // been computed

          if (keyHashSlot == -1)
          {
            keyHashSlot = HashSlotCalculator.CalculateHashSlot(key);
          }

          // Update the in-proces cache with the value retrieved from Redis

          _inProcessCache.Set(key, new LocalCacheEntry<T>((ushort) keyHashSlot, timestamp, value), DateTimeOffset.UtcNow.AddSeconds((double) results[1]));
        }
      }

      return (T) inProcessCacheEntry.Data;
    }


  }
}
