namespace NSnappy
{
	public static class CompressorTag
	{
		public const byte Literal = 0;
		public const byte Copy1ByteOffset = 1;
		public const int Copy2ByteOffset = 2;
	}
}