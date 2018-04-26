using System;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using RedisMultilevelCache;

namespace TestHarness
{
  class Program
  {
    enum Operation {  Read, Update };

    private const int KEY_COUNT = 1000;
    private const int ITERATION_COUNT = 100000;
    private const int MIN_DATA_SIZE = 512;
    private const int MAX_DATA_SIZE = 4096;

    private static Random _rnd = new Random();

    static void Main(string[] args)
    {
      if (args.Length < 1)
      {
        Console.WriteLine("Usage: TestHarness redis_address[:redis_port]");
        Console.WriteLine("Examples:");
        Console.WriteLine("  Use locally installed Redis server, default port of 6379:  TestHarness localhost");
        Console.WriteLine("  Use Redis cluster which includes a node at cacheserver, port 6382:  TestHarness cacheserver:6382");
        return;
      }

      var cache = new MultilevelCacheProvider(args[0]);

      Console.WriteLine("Loading cache data...");

      // Prefill the cache with data, using parallel operations to better 
      // simulate a multi-threaded client

      Parallel.For(0, KEY_COUNT, (i) =>
      {
        cache.Set(BuildKey(i), GetRandomData(), TimeSpan.FromMinutes(2));
      });

      Console.WriteLine("Executing test...");

      var stopwatch = Stopwatch.StartNew();

      long totalBytesTransfered = 0;

      // Run test loop in parallel, to better simulate multiple threads on a web
      // server accessing the cache

      Parallel.For(0, ITERATION_COUNT, (i) => {
        string key = GetRandomKey();
        Operation op = GetRandomOp();

        if (op == Operation.Update)
        {
          var data = GetRandomData();
          cache.Set(key, data, TimeSpan.FromMinutes(2));
          Interlocked.Add(ref totalBytesTransfered, data.Length);
        }
        else
        {
          var data = cache.Get<byte[]>(key);
          Interlocked.Add(ref totalBytesTransfered, data.Length);
        }
      });

      var elapsedTime = stopwatch.Elapsed;

      double opsPerSecond = ITERATION_COUNT / elapsedTime.TotalSeconds;
      double bytesPerSecond = totalBytesTransfered / elapsedTime.TotalSeconds;

      Console.WriteLine($"{opsPerSecond.ToString("N1")} op/sec");
      Console.WriteLine($"{bytesPerSecond.ToString("N1")} bytes/sec");
    }


    private static string GetRandomKey()
    {
      lock (_rnd)
      {
        return BuildKey(_rnd.Next(KEY_COUNT));
      }
    }


    /// <summary>
    /// Random op generator, generates 95% reads and 5% updates
    /// </summary>
    /// <returns></returns>
    private static Operation GetRandomOp()
    {
      Operation op = Operation.Read;

      lock (_rnd)
      {
        if (_rnd.NextDouble() > 0.95) op = Operation.Update;
      }

      return op;
    }


    private static byte[] GetRandomData()
    {
      byte[] data = null;

      lock (_rnd)
      {
        data = new byte[_rnd.Next(MAX_DATA_SIZE - MIN_DATA_SIZE) + MIN_DATA_SIZE];
        _rnd.NextBytes(data);
      }

      return data;
    }



    private static string BuildKey(int i)
    {
      return string.Concat("TestHarness:", i);
    }

  }
}
