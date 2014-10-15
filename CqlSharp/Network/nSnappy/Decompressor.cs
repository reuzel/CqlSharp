// CqlSharp - CqlSharp
// Copyright (c) 2014 Joost Reuzel
//   
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   
// http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Diagnostics;

namespace CqlSharp.Network.nSnappy
{
    public class Decompressor
    {
        private static readonly ushort[] CharTable =
        {
            0x0001, 0x0804, 0x1001, 0x2001, 0x0002, 0x0805, 0x1002, 0x2002,
            0x0003, 0x0806, 0x1003, 0x2003, 0x0004, 0x0807, 0x1004, 0x2004,
            0x0005, 0x0808, 0x1005, 0x2005, 0x0006, 0x0809, 0x1006, 0x2006,
            0x0007, 0x080a, 0x1007, 0x2007, 0x0008, 0x080b, 0x1008, 0x2008,
            0x0009, 0x0904, 0x1009, 0x2009, 0x000a, 0x0905, 0x100a, 0x200a,
            0x000b, 0x0906, 0x100b, 0x200b, 0x000c, 0x0907, 0x100c, 0x200c,
            0x000d, 0x0908, 0x100d, 0x200d, 0x000e, 0x0909, 0x100e, 0x200e,
            0x000f, 0x090a, 0x100f, 0x200f, 0x0010, 0x090b, 0x1010, 0x2010,
            0x0011, 0x0a04, 0x1011, 0x2011, 0x0012, 0x0a05, 0x1012, 0x2012,
            0x0013, 0x0a06, 0x1013, 0x2013, 0x0014, 0x0a07, 0x1014, 0x2014,
            0x0015, 0x0a08, 0x1015, 0x2015, 0x0016, 0x0a09, 0x1016, 0x2016,
            0x0017, 0x0a0a, 0x1017, 0x2017, 0x0018, 0x0a0b, 0x1018, 0x2018,
            0x0019, 0x0b04, 0x1019, 0x2019, 0x001a, 0x0b05, 0x101a, 0x201a,
            0x001b, 0x0b06, 0x101b, 0x201b, 0x001c, 0x0b07, 0x101c, 0x201c,
            0x001d, 0x0b08, 0x101d, 0x201d, 0x001e, 0x0b09, 0x101e, 0x201e,
            0x001f, 0x0b0a, 0x101f, 0x201f, 0x0020, 0x0b0b, 0x1020, 0x2020,
            0x0021, 0x0c04, 0x1021, 0x2021, 0x0022, 0x0c05, 0x1022, 0x2022,
            0x0023, 0x0c06, 0x1023, 0x2023, 0x0024, 0x0c07, 0x1024, 0x2024,
            0x0025, 0x0c08, 0x1025, 0x2025, 0x0026, 0x0c09, 0x1026, 0x2026,
            0x0027, 0x0c0a, 0x1027, 0x2027, 0x0028, 0x0c0b, 0x1028, 0x2028,
            0x0029, 0x0d04, 0x1029, 0x2029, 0x002a, 0x0d05, 0x102a, 0x202a,
            0x002b, 0x0d06, 0x102b, 0x202b, 0x002c, 0x0d07, 0x102c, 0x202c,
            0x002d, 0x0d08, 0x102d, 0x202d, 0x002e, 0x0d09, 0x102e, 0x202e,
            0x002f, 0x0d0a, 0x102f, 0x202f, 0x0030, 0x0d0b, 0x1030, 0x2030,
            0x0031, 0x0e04, 0x1031, 0x2031, 0x0032, 0x0e05, 0x1032, 0x2032,
            0x0033, 0x0e06, 0x1033, 0x2033, 0x0034, 0x0e07, 0x1034, 0x2034,
            0x0035, 0x0e08, 0x1035, 0x2035, 0x0036, 0x0e09, 0x1036, 0x2036,
            0x0037, 0x0e0a, 0x1037, 0x2037, 0x0038, 0x0e0b, 0x1038, 0x2038,
            0x0039, 0x0f04, 0x1039, 0x2039, 0x003a, 0x0f05, 0x103a, 0x203a,
            0x003b, 0x0f06, 0x103b, 0x203b, 0x003c, 0x0f07, 0x103c, 0x203c,
            0x0801, 0x0f08, 0x103d, 0x203d, 0x1001, 0x0f09, 0x103e, 0x203e,
            0x1801, 0x0f0a, 0x103f, 0x203f, 0x2001, 0x0f0b, 0x1040, 0x2040
        };

        private static readonly uint[] Wordmask =
        {
            0u, 0xffu, 0xffffu, 0xffffffu, 0xffffffffu
        };

        private Pointer _ip;
        private int _ipLimit;
        private Writer _output;
        private int _peeked;

        private Decompressor()
        {
        }

        public static int Decompress(byte[] input, int compressedSize, out byte[] output)
        {
            var decompressor = new Decompressor();
            return decompressor.RawUncompress(input, compressedSize, out output);
        }

        private int RawUncompress(byte[] input, int compressedSize, out byte[] output)
        {
            _ip = new Pointer(input);

            //set limit to pointer scope
            _ipLimit = compressedSize;

            //get size of uncompressed data
            var uncompressedSize = (int)ReadUncompressedLength();
            _output = new Writer(uncompressedSize);

            //decompress
            DecompressAllTags();

            //return results
            output = _output.ToArray();
            return uncompressedSize;
        }

        private void DecompressAllTags()
        {
            Pointer ip = _ip;

            // We could have put this refill fragment only at the beginning of the loop.
            // However, duplicating it at the end of each branch gives the compiler more
            // scope to optimize the <ip_limit_ - ip> expression based on the local
            // context, which overall increases speed.

            // ReSharper disable AccessToModifiedClosure
            Func<bool> maybeRefill = () =>
            {
                if(_ipLimit - ip < 5)
                {
                    _ip = ip;
                    if(!RefillTag())
                        return false;

                    ip = _ip;
                }

                return true;
            };
            // ReSharper restore AccessToModifiedClosure

            if(!maybeRefill())
                return;

            for(;;)
            {
                byte c = ip[0];
                ip = ip + 1;

                if((c & 0x3) == CompressorTag.Literal)
                {
                    int literalLength = ((c >> 2) + 1);
                    if(_output.TryFastAppend(ip, _ipLimit - ip, literalLength))
                    {
                        Debug.Assert(literalLength < 61);
                        ip += literalLength;
                        if(!maybeRefill())
                            return;

                        continue;
                    }

                    if(literalLength >= 61)
                    {
                        int longLiteral = literalLength - 60;
                        literalLength = (int)((ip.ToUInt32() & Wordmask[longLiteral]) + 1);
                        ip += longLiteral;
                    }

                    int avail = _ipLimit - ip;
                    while(avail < literalLength)
                    {
                        if(!_output.Append(ip, avail))
                            return;
                        literalLength -= avail;

                        Skip(_peeked);
                        int n;
                        ip = Peek(out n);
                        avail = n;
                        _peeked = avail;
                        if(avail == 0)
                            return; // Premature end of input

                        _ipLimit = ip + avail;
                    }
                    if(!_output.Append(ip, literalLength))
                        return;
                    ip += literalLength;
                    if(!maybeRefill())
                        return;
                }
                else
                {
                    int entry = CharTable[c];
                    var trailer = (int)(ip.ToUInt32() & Wordmask[entry >> 11]);
                    int length = entry & 0xff;
                    ip += entry >> 11;

                    // copy_offset/256 is encoded in bits 8..10.  By just fetching
                    // those bits, we get copy_offset (since the bit-field starts at
                    // bit 8).
                    int copyOffset = entry & 0x700;
                    if(!_output.AppendFromSelf(copyOffset + trailer, length))
                        return;
                    if(!maybeRefill())
                        return;
                }
            }
        }

        private Pointer Peek(out int i)
        {
            i = _ipLimit - _ip;
            return _ip;
        }

        private void Skip(int peeked)
        {
            _ip += peeked;
        }

        // Data stored per entry in lookup table:
        //      Range   Bits-used       Description
        //      ------------------------------------
        //      1..64   0..7            Literal/copy length encoded in opcode byte
        //      0..7    8..10           Copy offset encoded in opcode byte / 256
        //      0..4    11..13          Extra bytes after opcode
        //
        // We use eight bits for the length even though 7 would have sufficed
        // because of efficiency reasons:
        //      (1) Extracting a byte is faster than a bit-field
        //      (2) It properly aligns copy offset so we do not need a <<8

        private bool RefillTag()
        {
            return _ip < _ipLimit;
        }

        private uint ReadUncompressedLength()
        {
            // Length is encoded in 1..5 bytes
            uint result = 0;
            uint shift = 0;
            while(true)
            {
                if(shift >= 32)
                    return 0;

                byte c = _ip[0];
                _ip = _ip + 1;

                result |= (c & 0x7fu) << (int)shift;
                if(c < 128)
                    break;

                shift += 7;
            }

            return result;
        }
    }
}