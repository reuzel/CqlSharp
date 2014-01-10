// CqlSharp - CqlSharp
// Copyright (c) 2013 Joost Reuzel
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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

namespace CqlSharp
{
    /// <summary>
    ///   DateTime extensions to convert date-time values to and from unix-time
    /// </summary>
    public static class TypeExtensions
    {
        public static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        ///   Translates the DateTime to a unix/POSIX timestamp
        /// </summary>
        /// <param name="datetime"> The datetime. </param>
        /// <returns> </returns>
        public static long ToTimestamp(this DateTime datetime)
        {
            return (long)datetime.ToUniversalTime().Subtract(Epoch).TotalMilliseconds;
        }

        /// <summary>
        ///   Translates a unix/POSIX timestamp to a DateTime
        /// </summary>
        /// <param name="timestamp"> The timestamp. </param>
        /// <returns> </returns>
        public static DateTime ToDateTime(this long timestamp)
        {
            return Epoch.AddMilliseconds(timestamp);
        }


        /// <summary>
        ///   converts the array into a long value (big-endian)
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
        ///   converts the array into a int value (big-endian)
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
        ///   converts the array into a unsigned short value (big-endian)
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
        ///   converts the array to a Guid value
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
        /// Hex string lookup table.
        /// </summary>
        private static readonly string[] HexStringTable = new string[]
        {
            "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0A", "0B", "0C", "0D", "0E", "0F",
            "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "1A", "1B", "1C", "1D", "1E", "1F",
            "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "2A", "2B", "2C", "2D", "2E", "2F",
            "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "3A", "3B", "3C", "3D", "3E", "3F",
            "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "4A", "4B", "4C", "4D", "4E", "4F",
            "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "5A", "5B", "5C", "5D", "5E", "5F",
            "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "6A", "6B", "6C", "6D", "6E", "6F",
            "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "7A", "7B", "7C", "7D", "7E", "7F",
            "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "8A", "8B", "8C", "8D", "8E", "8F",
            "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "9A", "9B", "9C", "9D", "9E", "9F",
            "A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "AA", "AB", "AC", "AD", "AE", "AF",
            "B0", "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "BA", "BB", "BC", "BD", "BE", "BF",
            "C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "CA", "CB", "CC", "CD", "CE", "CF",
            "D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "DA", "DB", "DC", "DD", "DE", "DF",
            "E0", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9", "EA", "EB", "EC", "ED", "EE", "EF",
            "F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "FA", "FB", "FC", "FD", "FE", "FF"
        };

        /// <summary>
        /// Returns a hex string representation of an array of bytes.
        /// </summary>
        /// <param name="value">The array of bytes.</param>
        /// <param name="prefix">string value to prefix hex string with (e.g. 0x)</param>
        /// <returns>A hex string representation of the array of bytes.</returns>
        /// <remarks>From: http://blogs.msdn.com/b/blambert/archive/2009/02/22/blambert-codesnip-fast-byte-array-to-hex-string-conversion.aspx </remarks>
        public static string ToHex(this byte[] value, string prefix = "")
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.Append(prefix);
            if (value != null)
            {
                foreach (byte b in value)
                {
                    stringBuilder.Append(HexStringTable[b]);
                }
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        ///   Determines whether the specified type is anonymous.
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
    }
}