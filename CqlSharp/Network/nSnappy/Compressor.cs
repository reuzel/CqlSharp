using CqlSharp.Memory;
using System;
using System.Diagnostics;
using System.IO;

namespace CqlSharp.Network.nSnappy
{
    public static class Compressor
    {
        public static int Compress(Stream input, Stream output)
        {
            var length = (int)(input.Length - input.Position);

            var varInt = new VarInt32(length).GetEncodedValue();
            output.Write(varInt, 0, varInt.Length);

            int bytesWritten = varInt.Length;

            int bytesToRead = Math.Min(length, CompressorConstants.BlockSize);

            var fragment = MemoryPool.Instance.Take(bytesToRead);

            int maxOutput = MaxCompressedOutput(bytesToRead);

            var block = MemoryPool.Instance.Take(maxOutput);

            while (length > 0)
            {
                var fragmentSize = input.Read(fragment, 0, bytesToRead);
                var hashTable = new HashTable(fragmentSize);

                int blockSize = CompressFragment(fragment, fragmentSize, hashTable, block);
                output.Write(block, 0, blockSize);
                bytesWritten += blockSize;

                length -= bytesToRead;
            }

            MemoryPool.Instance.Return(fragment);
            MemoryPool.Instance.Return(block);

            return bytesWritten;
        }

        private static int CompressFragment(byte[] source, int length, HashTable hashTable, byte[] scratchOutput)
        {
            const int inputMarginBytes = 15;

            int shift = 32 - Log2Floor(hashTable.Size);
            var op = new Pointer(scratchOutput);
            var ip = new Pointer(source);
            var nextEmit = new Pointer(source);
            var baseIp = new Pointer(ip);

            Func<Pointer, int, uint> hashPtr = (value, offset) => (value.ToUInt32(offset) * 0x1e35a7bd) >> shift;

            if (length >= inputMarginBytes)
            {
                var ipLimit = length - inputMarginBytes;

                ip = ip + 1;
                var nextHash = hashPtr(ip, 0);
                while (true)
                {
                    uint skip = 32;

                    var nextIp = new Pointer(ip);
                    Pointer candidate;
                    do
                    {
                        ip = nextIp;
                        uint hash = nextHash;
                        Debug.Assert(hash == hashPtr(ip, 0));

                        uint bytesBetweenHashLookups = skip++ >> 5;
                        nextIp = ip + bytesBetweenHashLookups;

                        if (nextIp > ipLimit)
                        {
                            goto emit_remainder;
                        }

                        nextHash = hashPtr(nextIp, 0);
                        candidate = baseIp + hashTable[hash];

                        Debug.Assert(candidate >= baseIp);
                        Debug.Assert(candidate < ip);

                        hashTable[hash] = ip - baseIp;

                    } while (ip.ToUInt32() != candidate.ToUInt32());

                    Debug.Assert(nextEmit + 16 <= length);

                    op = EmitLiteral(op, nextEmit, ip - nextEmit, allowFastPath: true);

                    Pointer inputBytes;
                    uint candidateBytes;

                    do
                    {
                        Pointer b = ip;
                        int matched = 4 + FindMatchLength(candidate + 4, ip + 4, length);
                        ip += matched;
                        var offset = b - candidate;

                        op = EmitCopy(op, offset, matched);

                        Pointer insertTail = ip - 1;
                        nextEmit = ip;

                        if (ip >= ipLimit)
                        {
                            goto emit_remainder;
                        }

                        inputBytes = insertTail;

                        var prevHash = hashPtr(inputBytes, 0);
                        hashTable[prevHash] = ip - baseIp - 1;

                        var curHash = hashPtr(inputBytes, 1);
                        candidate = baseIp + hashTable[curHash];

                        candidateBytes = candidate.ToUInt32();
                        hashTable[curHash] = ip - baseIp;

                    } while (inputBytes.ToUInt32(1) == candidateBytes);

                    nextHash = hashPtr(inputBytes, 2);
                    ip = ip + 1;
                }
            }

        emit_remainder:
            if (nextEmit < length)
            {
                op = EmitLiteral(op, nextEmit, length - nextEmit, false);
            }

            return op;
        }

        private static int FindMatchLength(Pointer p1, Pointer p2, int length)
        {
            int matched = 0;
            while (p2 <= length - 4 && p2.ToUInt32() == (p1 + matched).ToUInt32())
            {
                p2 += 4;
                matched += 4;
            }

            if (p2 <= length - 4)
            {
                var x = p2.ToUInt32() ^ (p1 + matched).ToUInt32();
                var matchingBits = FindLSBSetNonZero(x);
                matched += matchingBits >> 3;
            }
            else
            {
                while ((p2 < length) && (p1[matched] == p2[0]))
                {
                    p2 += 1;
                    ++matched;
                }
            }

            return matched;
        }

        private static int FindLSBSetNonZero(uint number)
        {
            int bit = 31;

            for (int i = 4, shift = 1 << 4; i >= 0; --i)
            {
                var x = number << shift;
                if (x != 0)
                {
                    number = x;
                    bit -= shift;
                }

                shift >>= 1;
            }

            return bit;
        }

        private static Pointer EmitCopy(Pointer op, int offset, int len)
        {
            // Emit 64 byte copies but make sure to keep at least four bytes reserved
            while (len >= 68)
            {
                op = EmitCopyLessThan64(op, offset, 64);
                len -= 64;
            }

            // Emit an extra 60 byte copy if have too much data to fit in one copy
            if (len > 64)
            {
                op = EmitCopyLessThan64(op, offset, 60);
                len -= 60;
            }

            // Emit remainder
            op = EmitCopyLessThan64(op, offset, len);
            return op;
        }

        private static Pointer EmitCopyLessThan64(Pointer op, int offset, int len)
        {
            Debug.Assert(len <= 64);
            Debug.Assert(len >= 4);
            Debug.Assert(offset < 65536);

            if ((len < 12) && (offset < 2048))
            {
                int lenMinus4 = len - 4;
                Debug.Assert(lenMinus4 < 8); // Must fit in 3 bits

                op[0] = (byte)(CompressorTag.Copy1ByteOffset | ((lenMinus4) << 2) | ((offset >> 8) << 5));
                op[1] = (byte)(offset & 0xff);
                op = op + 2;
            }
            else
            {
                op[0] = (byte)(CompressorTag.Copy2ByteOffset | ((len - 1) << 2));
                op += 1;

                op.WriteUInt16(offset);
                op += 2;
            }

            return op;
        }

        private static Pointer EmitLiteral(Pointer dest, Pointer literal, int length, bool allowFastPath)
        {
            int n = length - 1;
            if (n < 60)
            {
                var value = CompressorTag.Literal | (n << 2);
                dest[0] = (byte)value;
                dest += 1;

                if (allowFastPath && length <= 16)
                {
                    dest.Copy64(literal);
                    dest.Copy64(literal + 8, offset: 8);
                    return dest + length;
                }
            }
            else
            {
                var tmp = new Pointer(dest);
                dest += 1;

                int count = 0;
                while (n > 0)
                {
                    dest[count] = (byte)(n & 0xff);
                    n >>= 8;
                    count++;
                }

                Debug.Assert(count >= 1);
                Debug.Assert(count <= 4);

                tmp[0] = (byte)(CompressorTag.Literal | (59 + count) << 2);
                dest += count;
            }

            dest.Copy(literal, length);
            return dest + length;
        }

        private static int Log2Floor(uint n)
        {
            if (n == 0)
                return -1;

            int log = 0;
            uint value = n;

            for (int i = 4; i >= 0; --i)
            {
                int shift = (1 << i);
                uint x = value >> shift;
                if (x != 0)
                {
                    value = x;
                    log += shift;
                }
            }

            Debug.Assert(value == 1);
            return log;
        }

        private static int MaxCompressedOutput(int size)
        {
            return 32 + size + size / 6;
        }
    }
}