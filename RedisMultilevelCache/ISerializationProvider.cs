using System.IO;


namespace RedisMultilevelCache
{
  /// <summary>
  /// This interface defines the methods needed to replace the RedisMultilevelCache's default serialization provider
  /// </summary>
  /// <remarks>
  /// Generic type parameters are used here to better accomodate serializers like Newtonsoft.Json,
  /// which need to know the type they're deserializing to
  /// </remarks>
  public interface ISerializationProvider
  {
    void Serialize<T>(Stream serializationStream, T data);
    T Deserialize<T>(Stream serializationStream);
  }
}
