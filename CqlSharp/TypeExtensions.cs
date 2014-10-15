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
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

namespace CqlSharp
{
    /// <summary>
    /// DateTime extensions to convert date-time values to and from unix-time
    /// </summary>
    public static class TypeExtensions
    {
        public static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Translates the DateTime to a unix/POSIX timestamp
        /// </summary>
        /// <param name="datetime"> The datetime. </param>
        /// <returns> </returns>
        public static long ToTimestamp(this DateTime datetime)
        {
            return (long)datetime.ToUniversalTime().Subtract(Epoch).TotalMilliseconds;
        }

        /// <summary>
        /// Translates a unix/POSIX timestamp to a DateTime
        /// </summary>
        /// <param name="timestamp"> The timestamp. </param>
        /// <returns> </returns>
        public static DateTime ToDateTime(this long timestamp)
        {
            return Epoch.AddMilliseconds(timestamp);
        }

        /// <summary>
        /// Writes the datetime as a unix timestamp to the provided array
        /// </summary>
        /// <param name="datetime">The datetime.</param>
        /// <param name="array">The array.</param>
        /// <param name="offset">The offset.</param>
        public static void ToBytes(this DateTime datetime, byte[] array, int offset = 0)
        {
            datetime.ToTimestamp().ToBytes(array, offset);
        }

        /// <summary>
        /// converts the array into a long value (big-endian)
        /// </summary>
        /// <param name="bytes"> The bytes. </param>
        /// <param name="offset"> The offset. </param>
        /// <returns> </returns>
        public static long ToLong(this byte[] bytes, int offset = 0)
        {
            long value = (long)bytes[offset] << 56;
            value |= (long)bytes[offset + 1] << 48;
            value |= (long)bytes[offset + 2] << 40;
            value |= (long)bytes[offset + 3] << 32;
            value |= (long)bytes[offset + 4] << 24;
            value |= (long)bytes[offset + 5] << 16;
            value |= (long)bytes[offset + 6] << 8;
            value |= bytes[offset + 7];

            return value;
        }

        /// <summary>
        /// Writes the long value to the provided array from the given offset onwards
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="array">The array.</param>
        /// <param name="offset">The offset.</param>
        public static void ToBytes(this long value, byte[] array, int offset = 0)
        {
            array[offset] = (byte)((value >> 56) & 0xff);
            array[offset + 1] = (byte)((value >> 48) & 0xff);
            array[offset + 2] = (byte)((value >> 40) & 0xff);
            array[offset + 3] = (byte)((value >> 32) & 0xff);
            array[offset + 4] = (byte)((value >> 24) & 0xff);
            array[offset + 5] = (byte)((value >> 16) & 0xff);
            array[offset + 6] = (byte)((value >> 8) & 0xff);
            array[offset + 7] = (byte)((value) & 0xff);
        }

        /// <summary>
        /// converts the array into a int value (big-endian)
        /// </summary>
        /// <param name="bytes"> The bytes. </param>
        /// <param name="offset"> The offset. </param>
        /// <returns> </returns>
        public static int ToInt(this byte[] bytes, int offset = 0)
        {
            int value = bytes[offset] << 24;
            value |= bytes[offset + 1] << 16;
            value |= bytes[offset + 2] << 8;
            value |= bytes[offset + 3];

            return value;
        }


        /// <summary>
        /// Writes the long value to the provided array from the given offset onwards
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="array">The array.</param>
        /// <param name="offset">The offset.</param>
        public static void ToBytes(this int value, byte[] array, int offset = 0)
        {
            array[offset] = (byte)((value >> 24) & 0xff);
            array[offset + 1] = (byte)((value >> 16) & 0xff);
            array[offset + 2] = (byte)((value >> 8) & 0xff);
            array[offset + 3] = (byte)((value) & 0xff);
        }


        /// <summary>
        /// converts the array into a unsigned short value (big-endian)
        /// </summary>
        /// <param name="bytes"> The bytes. </param>
        /// <param name="offset"> The offset. </param>
        /// <returns> </returns>
        public static ushort ToShort(this byte[] bytes, int offset = 0)
        {
            int value = bytes[offset] << 8 | bytes[offset + 1];
            return (ushort)value;
        }

        /// <summary>
        /// Writes the long value to the provided array from the given offset onwards
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="array">The array.</param>
        /// <param name="offset">The offset.</param>
        public static void ToBytes(this ushort value, byte[] array, int offset = 0)
        {
            array[offset] = (byte)((value >> 8) & 0xff);
            array[offset + 1] = (byte)((value) & 0xff);
        }

        /// <summary>
        /// converts the array to a Guid value
        /// </summary>
        /// <param name="bytes"> The bytes. </param>
        /// <param name="offset"> The offset. </param>
        /// <returns> </returns>
        public static Guid ToGuid(this byte[] bytes, int offset = 0)
        {
            unchecked
            {
                int a = bytes.ToInt(offset);
                var b = (short)bytes.ToShort(offset + 4);
                var c = (short)bytes.ToShort(offset + 6);

                return new Guid(a, b, c, bytes[offset + 8], bytes[offset + 9], bytes[offset + 10], bytes[offset + 11],
                                bytes[offset + 12], bytes[offset + 13],
                                bytes[offset + 14], bytes[offset + 15]);
            }
        }

        /// <summary>
        /// Writes the long value to the provided array from the given offset onwards
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="array">The array.</param>
        /// <param name="offset">The offset.</param>
        public static void ToBytes(this Guid value, byte[] array, int offset = 0)
        {
            byte[] rawData = value.ToByteArray();
            if(BitConverter.IsLittleEndian)
            {
                array[offset + 0] = rawData[3];
                array[offset + 1] = rawData[2];
                array[offset + 2] = rawData[1];
                array[offset + 3] = rawData[0];
                array[offset + 4] = rawData[5];
                array[offset + 5] = rawData[4];
                array[offset + 6] = rawData[7];
                array[offset + 7] = rawData[6];
                Buffer.BlockCopy(rawData, 8, array, offset + 8, 8);
            }
            else
                Buffer.BlockCopy(rawData, 0, array, offset, 16);
        }

        /// <summary>
        /// Hex string lookup table.
        /// </summary>
        private static readonly string[] HexStringTable =
        {
            "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0a", "0b", "0c", "0d", "0e", "0f",
            "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "1a", "1b", "1c", "1d", "1e", "1f",
            "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "2a", "2b", "2c", "2d", "2e", "2f",
            "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "3a", "3b", "3c", "3d", "3e", "3f",
            "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "4a", "4b", "4c", "4d", "4e", "4f",
            "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "5a", "5b", "5c", "5d", "5e", "5f",
            "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "6a", "6b", "6c", "6d", "6e", "6f",
            "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "7a", "7b", "7c", "7d", "7e", "7f",
            "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "8a", "8b", "8c", "8d", "8e", "8f",
            "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "9a", "9b", "9c", "9d", "9e", "9f",
            "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "aa", "ab", "ac", "ad", "ae", "af",
            "b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9", "ba", "bb", "bc", "bd", "be", "bf",
            "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "ca", "cb", "cc", "cd", "ce", "cf",
            "d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "da", "db", "dc", "dd", "de", "df",
            "e0", "e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8", "e9", "ea", "eb", "ec", "ed", "ee", "ef",
            "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "fa", "fb", "fc", "fd", "fe", "ff"
        };

        /// <summary>
        /// Returns a hex string representation of an array of bytes.
        /// </summary>
        /// <param name="value">The array of bytes.</param>
        /// <param name="prefix">string value to prefix hex string with (e.g. 0x)</param>
        /// <returns>A hex string representation of the array of bytes.</returns>
        /// <remarks>
        /// From:
        /// http://blogs.msdn.com/b/blambert/archive/2009/02/22/blambert-codesnip-fast-byte-array-to-hex-string-conversion.aspx
        /// </remarks>
        public static string ToHex(this byte[] value, string prefix = "")
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.Append(prefix);
            if(value != null)
            {
                foreach(byte b in value)
                    stringBuilder.Append(HexStringTable[b]);
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Returns a hex string representation of an array of bytes.
        /// </summary>
        /// <param name="value">The array of bytes.</param>
        /// <param name="prefix">string value to prefix hex string with (e.g. 0x)</param>
        /// <returns>A hex string representation of the array of bytes.</returns>
        /// <remarks>
        /// From:
        /// http://blogs.msdn.com/b/blambert/archive/2009/02/22/blambert-codesnip-fast-byte-array-to-hex-string-conversion.aspx
        /// </remarks>
        public static string EncodeAsHex(this string value, string prefix = "")
        {
            var stringBuilder = new StringBuilder(prefix);
            if(value != null)
            {
                foreach(char b in value)
                    stringBuilder.Append(HexStringTable[b]);
            }

            return stringBuilder.ToString();
        }


        /// <summary>
        /// Decodes the hexadecimal string into its normal form.
        /// </summary>
        /// <param name="hex">The hexadecimal string.</param>
        /// <returns></returns>
        /// <exception cref="CqlException">Error parsing hexadecimal string. A hex string must have an even number of digits.</exception>
        public static string DecodeHex(this string hex)
        {
            if(hex.Length%2 != 0)
            {
                throw new CqlException(
                    "Error parsing hexadecimal string. A hex string must have an even number of digits.");
            }

            StringBuilder sb = new StringBuilder();

            for(int i = 0; i < hex.Length; i = i + 2)
            {
                char c = (char)((GetHexVal(hex[i]) << 4) | GetHexVal(hex[i + 1]));
                sb.Append(c);
            }

            return sb.ToString();
        }

        private static int GetHexVal(char hex)
        {
            int val = hex;
            //For uppercase A-F letters:
            //return val - (val < 58 ? 48 : 55);
            //For lowercase a-f letters:
            //return val - (val < 58 ? 48 : 87);
            //Or the two combined, but a bit slower:
            return val - (val < 58 ? 48 : (val < 97 ? 55 : 87));
        }

        /// <summary>
        /// Determines whether the specified type is anonymous.
        /// </summary>
        /// <param name="type"> The type. </param>
        /// <returns> <c>true</c> if the specified type is anonymous; otherwise, <c>false</c> . </returns>
        public static bool IsAnonymous(this Type type)
        {
            return Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false)
                   && type.IsGenericType && type.Name.Contains("AnonymousType")
                   &&
                   (type.Name.StartsWith("<>", StringComparison.OrdinalIgnoreCase) ||
                    type.Name.StartsWith("VB$", StringComparison.OrdinalIgnoreCase))
                   && (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
        }

        internal static readonly List<Type> TupleTypes = new List<Type>
        {
            typeof(Tuple<>),
            typeof(Tuple<,>),
            typeof(Tuple<,,>),
            typeof(Tuple<,,,>),
            typeof(Tuple<,,,,>),
            typeof(Tuple<,,,,,>),
            typeof(Tuple<,,,,,,>),
            typeof(Tuple<,,,,,,>),
            typeof(Tuple<,,,,,,,>)
        };
    }
}