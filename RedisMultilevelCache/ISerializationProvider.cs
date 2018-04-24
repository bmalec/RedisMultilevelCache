using System.IO;


namespace RedisMultilevelCache
{
  /// <summary>
  /// This interface defines the methods needed to replace the RedisMultilevelCache's default serialization provider
  /// </summary>
  public interface ISerializationProvider
  {
    void Serialize<T>(Stream serializationStream, T data);
    T Deserialize<T>(Stream serializationStream);
  }
}
