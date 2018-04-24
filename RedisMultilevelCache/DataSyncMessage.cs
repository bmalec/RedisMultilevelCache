using System;


namespace RedisMultilevelCache
{
  internal class DataSyncMessage
  {
    public Guid SenderInstanceId { get; private set; }
    public ushort KeyHashSlot { get; private set; }


    private DataSyncMessage(Guid senderInstanceId, ushort keyHashSlot)
    {
      SenderInstanceId = senderInstanceId;
      KeyHashSlot = keyHashSlot;
    }

    public static DataSyncMessage Create(Guid senderInstanceId, ushort keyHashSlot)
    {
      return new DataSyncMessage(senderInstanceId, keyHashSlot);
    }

    public byte[] Serialize()
    {
      var messageBytes = new byte[18];

      Buffer.BlockCopy(SenderInstanceId.ToByteArray(), 0, messageBytes, 0, 16);

      // final two bytes are the key CRC in big-endian format

      messageBytes[16] = (byte)(KeyHashSlot >> 8);
      messageBytes[17] = (byte)(KeyHashSlot & 0x00FF);

      return messageBytes;
    }

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
