using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace RedisMultilevelCache
{
  /// <summary>
  /// Default serialization provider implementation
  /// </summary>
  /// <remarks>
  /// This implementation requires the [Serializeable] attribute on all all classes stored in the cache
  /// </remarks>
  internal class DefaultSerializationProvider : ISerializationProvider
  {
    public T Deserialize<T>(Stream serializationStream)
    {
      var formatter = new BinaryFormatter();
      return (T) formatter.Deserialize(serializationStream);
    }

    public void Serialize<T>(Stream serializationStream, T data)
    {
      var formatter = new BinaryFormatter();
      formatter.Serialize(serializationStream, data);
    }
  }
}
