using System;
using System.Linq;
using System.IO;

namespace CqlSharp.Network.Snappy
{
    /// <summary>
    /// Happily copied from Snappy.Sharp: https://github.com/jeffesp/Snappy.Sharp
    /// </summary>
    public static class Snappy
    {
        internal const int Literal = 0;
        internal const int Copy1ByteOffset = 1; // 3 bit length + 3 bits of offset in opcode
        internal const int Copy2ByteOffset = 2;
        internal const int Copy4ByteOffset = 3;

        public static int MaxCompressedLength(int sourceLength)
        {
            var compressor = new SnappyCompressor();
            return compressor.MaxCompressedLength(sourceLength);
        }

        public static byte[] Compress(byte[] uncompressed)
        {
            var target = new SnappyCompressor();
            var result = new byte[target.MaxCompressedLength(uncompressed.Length)];
            var count = target.Compress(uncompressed, 0, uncompressed.Length, result);
            return result.Take(count).ToArray();
        }
        
        public static int GetUncompressedLength(byte[] compressed, int offset = 0)
        {
            var decompressor = new SnappyDecompressor();
            return decompressor.ReadUncompressedLength(compressed, offset)[0];
        }
    }
}
