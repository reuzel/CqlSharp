namespace NSnappy
{
	public struct VarInt32
	{
		private const int MAX_ENCODED_BYTES = 5;

		public int Value { get; private set; }

		public VarInt32(int value)
			: this()
		{
			Value = value;
		}

		public VarInt32(byte[] data)
			: this()
		{
			Value = IntEncoder.Decode(data, MAX_ENCODED_BYTES);
		}

		public byte[] GetEncodedValue()
		{
			return IntEncoder.Encode(Value);
		}
	}
}