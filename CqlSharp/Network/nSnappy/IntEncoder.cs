namespace NSnappy
{
	public static class IntEncoder
	{
		public static byte[] Encode(int value)
		{
			const int moreData = 128;
			var uvalue = unchecked((uint) value);

			if (uvalue < 0x80)
			{
				return new[] {(byte) uvalue};
			}

			if (uvalue < 0x4000)
			{
				return new[] {(byte) (uvalue | moreData), (byte) (uvalue >> 7)};
			}

			if (uvalue < 0x200000)
			{
				return new[] {(byte) (uvalue | moreData), (byte) ((uvalue >> 7) | moreData), (byte) (uvalue >> 14)};
			}

			if (uvalue < 0x10000000)
			{
				return new[] {(byte) (uvalue | moreData), (byte) ((uvalue >> 7) | moreData), (byte) ((uvalue >> 14) | moreData), (byte) (uvalue >> 21)};
			}

			return new[] {(byte) (uvalue | moreData), (byte) ((uvalue >> 7) | moreData), (byte) ((uvalue >> 14) | moreData), (byte) ((uvalue >> 21) | moreData), (byte) (uvalue >> 28)};
		}

		public static int Decode(byte[] data, int maxEncodedBytes)
		{
			var index = 0;
			var value = 0U;

			while (index < maxEncodedBytes)
			{
				var b = data[index];
				value |= (b & 0x7fU) << index*7;

				if (b < 0x80)
					break;

				index++;
			}

			return unchecked((int) value);
		}
	}
}