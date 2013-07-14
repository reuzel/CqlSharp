namespace NSnappy
{
	public static class CompressorConstants
	{
		public const int MaxIncrementCopyOverflow = 10;
		public const int BlockLog = 15;
		public const int BlockSize = 1 << BlockLog;

		public const int MaxHashTableBits = 14;
		public const int MaxHashTableSize = 1 << MaxHashTableBits;
	}
}