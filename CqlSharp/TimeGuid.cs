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
using System.Threading;

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
        private static readonly object Lock = new object();
        
        // multiplex version info
        private const int VersionByte = 7;
        private const int VersionByteMask = 0x0f;
        private const int VersionByteShift = 4;

        // offset to move from 1/1/0001, which is 0-time for .NET, to gregorian 0-time of 10/15/1582
        private static readonly DateTime GregorianCalendarStart = new DateTime(1582, 10, 15, 0, 0, 0, DateTimeKind.Utc);

        private static readonly Random Random = new Random((int)DateTime.UtcNow.Ticks);

        // random node that is 16 bytes
        private static byte[] _nodeId;

        // sequence numbers, etc
        private static long _lastTime = 0L;
        private static ushort _clockSequenceNumber = 1;
        private static int _timeSequenceNumber = 0;

        static TimeGuid()
        {
            _nodeId = CreateNodeId();
        }

        private static byte[] CreateNodeId()
        {
            var nodeId = new byte[6];
            Random.NextBytes(nodeId);

            // turn on I/G and U/L bits (per the node spec)
            nodeId[0] |= 0xC0;

            return nodeId;
        }

        public static DateTime GetDateTime(this Guid guid)
        {
            byte[] bytes = guid.ToByteArray();

            // reverse the version
            bytes[VersionByte] &= VersionByteMask;
            bytes[VersionByte] |= (byte)GuidVersion.TimeBased >> VersionByteShift;

            var timestampBytes = new byte[8];
            Array.Copy(bytes, 0, timestampBytes, 0, 8);

            long timestamp = BitConverter.ToInt64(timestampBytes, 0);
            long ticks = timestamp + GregorianCalendarStart.Ticks;

            return new DateTime(ticks, DateTimeKind.Utc);
        }

        public static Guid GenerateTimeBasedGuid(this DateTime dateTime)
        {
            dateTime = dateTime.ToUniversalTime();
            var time = (long)(dateTime - GregorianCalendarStart).TotalMilliseconds;
            byte[] nodeId;
            int sequence;

            lock(Lock)
            {
                //generate a new nodeId when clock is set backward (to avoid collisions with earlier created uuids)
                if (time < _lastTime)
                    _nodeId = CreateNodeId();

                //if time changed, reset sequence numbers
                if (time != _lastTime)
                {
                    _clockSequenceNumber = 0;
                    _timeSequenceNumber = 0;
                }
                else
                {
                    //increment clockSequence, or timeSequence if we are out of clockSequences
                    if (_clockSequenceNumber == ushort.MaxValue)
                    {
                        _clockSequenceNumber = 0;
                        _timeSequenceNumber = (_timeSequenceNumber + 1) % 10000;
                    }
                    else
                        _clockSequenceNumber++;
                }

                //cache this time
                _lastTime = time;

                //capture values
                time = time * 10000 + _timeSequenceNumber;
                sequence = _clockSequenceNumber;
                nodeId = _nodeId;
            }


            var timeLow = (int)(time);
            var timeMid = (short)(time >> 32);
            var timeHiAndVersion = (short)(((time >> 48) & 0x0FFF) | (1 << 12));
            var seqLow = (byte)(sequence);
            var seqHiAndReserved = (byte)(((sequence & 0x3F00) >> 8) | 0x80);

            var guid = new Guid(timeLow, timeMid, timeHiAndVersion, seqLow, seqHiAndReserved, nodeId[0], nodeId[1], nodeId[2], nodeId[3], nodeId[4], nodeId[5]);

            return guid;
        }
    }
}