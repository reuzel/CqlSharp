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

namespace CqlSharp
{
    /// <summary>
    ///   DateTime extensions to convert date-time values to and from unix-time
    /// </summary>
    internal static class TypeExtensions
    {
        public static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        ///   Translates the DateTime to a unix/POSIX timestamp
        /// </summary>
        /// <param name="datetime"> The datetime. </param>
        /// <returns> </returns>
        public static long ToTimestamp(this DateTime datetime)
        {
            return (long) datetime.ToUniversalTime().Subtract(Epoch).TotalMilliseconds;
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
            long value = (long) bytes[offset] << 56;
            value |= (long) bytes[offset + 1] << 48;
            value |= (long) bytes[offset + 2] << 40;
            value |= (long) bytes[offset + 3] << 32;
            value |= (long) bytes[offset + 4] << 24;
            value |= (long) bytes[offset + 5] << 16;
            value |= (long) bytes[offset + 6] << 8;
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
            return (ushort) value;
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
                var b = (short) bytes.ToShort(offset + 4);
                var c = (short) bytes.ToShort(offset + 6);

                return new Guid(a, b, c, bytes[offset + 8], bytes[offset + 9], bytes[offset + 10], bytes[offset + 11],
                                bytes[offset + 12], bytes[offset + 13],
                                bytes[offset + 14], bytes[offset + 15]);
            }
        }
    }
}