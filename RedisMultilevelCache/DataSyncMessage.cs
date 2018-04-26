using System;


namespace RedisMultilevelCache
{
  /// <summary>
  /// Represents a notification to evict keys with a specific hashslot
  /// </summary>
  internal class DataSyncMessage
  {
    // Unique ID of the cache instance that initiated the data change
    public Guid SenderInstanceId { get; private set; }
    // Hashslot of the keys to evict
    public ushort KeyHashSlot { get; private set; }


    /// <summary>
    /// Private constructor to insure users call the static Create() method to create message objects
    /// </summary>
    private DataSyncMessage(Guid senderInstanceId, ushort keyHashSlot)
    {
      SenderInstanceId = senderInstanceId;
      KeyHashSlot = keyHashSlot;
    }

    /// <summary>
    /// Create a populated DataSyncMessage object
    /// </summary>
    /// <param name="senderInstanceId">ID of the cache instance making the data change</param>
    /// <param name="keyHashSlot">Hashslot of the keys being evicted</param>
    /// <returns></returns>
    public static DataSyncMessage Create(Guid senderInstanceId, ushort keyHashSlot)
    {
      return new DataSyncMessage(senderInstanceId, keyHashSlot);
    }


    /// <summary>
    /// Serialize the DataSyncMessage object to a byte array 
    /// </summary>
    /// <returns>Byte array representing a DataSyncMessage instance</returns>
    public byte[] Serialize()
    {
      var messageBytes = new byte[18];

      Buffer.BlockCopy(SenderInstanceId.ToByteArray(), 0, messageBytes, 0, 16);

      // final two bytes are the key CRC in big-endian format

      messageBytes[16] = (byte)(KeyHashSlot >> 8);
      messageBytes[17] = (byte)(KeyHashSlot & 0x00FF);

      return messageBytes;
    }

    /// <summary>
    /// Create a DataSyncMessage object from a byte array
    /// </summary>
    /// <param name="messageBytes">Byte array representing the DataSyncMessage object</param>
    /// <returns></returns>
    public static DataSyncMessage Deserialize(byte[] messageBytes)
    {
      if (messageBytes == null) throw new ArgumentNullException(nameof(messageBytes));
      if (messageBytes.Length != 18) throw new ArgumentException("Invalid message length");

      var guidBytes = new byte[16];
      Buffer.BlockCopy(messageBytes, 0, guidBytes, 0, guidBytes.Length);

      var senderInstanceId = new Guid(guidBytes);
      ushort keyHashSlot = (ushort)((((ushort) messageBytes[16]) << 8) + messageBytes[17]);

      return new DataSyncMessage(senderInstanceId, keyHashSlot);
    }


  }
}
