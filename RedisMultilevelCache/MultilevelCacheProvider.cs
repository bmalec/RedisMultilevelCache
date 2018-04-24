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
      public ushort KeyHashSlot { get; private set; }
      public long Timestamp { get; private set; }
      public T Data { get; private set; }

      public LocalCacheEntry(ushort keyHashSlot, long timestamp, T data)
      {
        KeyHashSlot = keyHashSlot;
        Timestamp = timestamp;
        Data = data;
      }
    }

    private readonly Guid _instanceId = Guid.NewGuid();
    private readonly ISerializationProvider _serializationProvider;

    /// <summary>
    /// This array stores the timestamp that a key with the corresponding hash slot was last updated
    /// </summary>
    /// <remarks>
    /// .Net will initialize all items to zero, which is a fine default for us
    /// </remarks>
    private readonly long[] _lastUpdated = new long[HASH_SLOT_COUNT];  // the array element index is the hash slot; for hash slot 3142 look up _lastUpdatedDictionary[3142]

    private readonly ConnectionMultiplexer _redisConnection;
    private readonly IDatabase _redisDb;
    private readonly MemoryCache _inProcessCache = MemoryCache.Default;



    public MultilevelCacheProvider(string redisConnectionString) : this(redisConnectionString, new DefaultSerializationProvider())
    {
    }


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
    /// <param name="channel"></param>
    /// <param name="message"></param>
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

      // Get the current timestamp before we do anything else.  Decrement it by one so any sync messages processed after this line will force
      // an immediate expiration of the key's hash slot

      var timestamp = Stopwatch.GetTimestamp() - 1;

      var keyHashSlot = HashSlotCalculator.CalculateHashSlot(key);

      byte[] serializedData = null;

      // Serialize the data and write to Redis

      using (MemoryStream ms = new MemoryStream())
      {
        _serializationProvider.Serialize<T>(ms, value);
        serializedData = ms.ToArray();
      }

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


    public T Get<T>(string key)
    {
      var timestamp = Stopwatch.GetTimestamp() - 1;

      int keyHashSlot = -1; 

      // attempt to retreive value from the in-process cache

      var inProcessCacheEntry = _inProcessCache.Get(key) as LocalCacheEntry<T>;

      if (inProcessCacheEntry != null)
      {
        // found the entry in the in-process cache, now
        // need to check if the entry may be stale and we
        // need to re-read from Redis

        keyHashSlot = HashSlotCalculator.CalculateHashSlot(key);

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
      // get the correct value

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
          using (MemoryStream ms = new MemoryStream(serializedData))
          {
            value = _serializationProvider.Deserialize<T>(ms);
          }

          if (keyHashSlot == -1)
          {
            keyHashSlot = HashSlotCalculator.CalculateHashSlot(key);
          }

          _inProcessCache.Set(key, new LocalCacheEntry<T>((ushort) keyHashSlot, timestamp, value), DateTimeOffset.UtcNow.AddSeconds((double) results[1]));
        }
      }



      return (T) inProcessCacheEntry.Data;

    }


  }
}
