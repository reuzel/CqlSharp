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
    ///   Helper class to generate Time based GUIDs.
    ///   <remarks>
    ///     Thanks to <a href="https://github.com/pchalamet/cassandra-sharp">Casssandra-Sharp</a> project.
    ///   </remarks>
    /// </summary>
    public static class TimeGuid
    {
        // number of bytes in guid
        private const int ByteArraySize = 16;

        // multiplex variant info
        private const int VariantByte = 8;

        private const int VariantByteMask = 0x3f;

        private const int VariantByteShift = 0x80;

        // multiplex version info
        private const int VersionByte = 7;

        private const int VersionByteMask = 0x0f;

        private const int VersionByteShift = 4;

        // indexes within the uuid array for certain boundaries
        private const byte TimestampByte = 0;

        private const byte GuidClockSequenceByte = 8;

        private const byte NodeByte = 10;

        // offset to move from 1/1/0001, which is 0-time for .NET, to gregorian 0-time of 10/15/1582
        private static readonly DateTime GregorianCalendarStart = new DateTime(1582, 10, 15, 0, 0, 0, DateTimeKind.Utc);

        // random node that is 16 bytes
        private static readonly byte[] RandomNode;

        private static readonly Random Random = new Random();

        static TimeGuid()
        {
            RandomNode = new byte[6];
            Random.NextBytes(RandomNode);
        }

        public static DateTime GetDateTime(this Guid guid)
        {
            byte[] bytes = guid.ToByteArray();

            // reverse the version
            bytes[VersionByte] &= VersionByteMask;
            bytes[VersionByte] |= (byte) GuidVersion.TimeBased >> VersionByteShift;

            var timestampBytes = new byte[8];
            Array.Copy(bytes, TimestampByte, timestampBytes, 0, 8);

            long timestamp = BitConverter.ToInt64(timestampBytes, 0);
            long ticks = timestamp + GregorianCalendarStart.Ticks;

            return new DateTime(ticks, DateTimeKind.Utc);
        }

        public static Guid GenerateTimeBasedGuid(this DateTime dateTime)
        {
            return GenerateTimeBasedGuid(dateTime, RandomNode);
        }

        public static Guid GenerateTimeBasedGuid(DateTime dateTime, byte[] node)
        {
            dateTime = dateTime.ToUniversalTime();
            long ticks = (dateTime - GregorianCalendarStart).Ticks;

            var guid = new byte[ByteArraySize];
            byte[] clockSequenceBytes = BitConverter.GetBytes(Convert.ToInt16(Environment.TickCount%Int16.MaxValue));
            byte[] timestamp = BitConverter.GetBytes(ticks);

            // copy node
            Array.Copy(node, 0, guid, NodeByte, Math.Min(6, node.Length));

            // copy clock sequence
            Array.Copy(clockSequenceBytes, 0, guid, GuidClockSequenceByte, Math.Min(2, clockSequenceBytes.Length));

            // copy timestamp
            Array.Copy(timestamp, 0, guid, TimestampByte, Math.Min(8, timestamp.Length));

            // set the variant
            guid[VariantByte] &= VariantByteMask;
            guid[VariantByte] |= VariantByteShift;

            // set the version
            guid[VersionByte] &= VersionByteMask;
            guid[VersionByte] |= (byte) GuidVersion.TimeBased << VersionByteShift;

            return new Guid(guid);
        }
    }
}