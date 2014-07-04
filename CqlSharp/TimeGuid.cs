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

namespace CqlSharp
{
    /// <summary>
    /// Helper class to generate Time based GUIDs.
    /// </summary>
    /// <remarks>
    /// The TimeGuid generation does not completely follow the RFC4122 recommendations, to allow
    /// for more Guids to be created per 100ns time frame. The used algorithm will increment the
    /// clockId when a uuid is requested with the same timestamp as the previous. When the clockId
    /// runs out (it has 2^14 possible values), the provided timestamp is incremented by 1, and the
    /// clockId is reset.
    /// When the clock is set backwards, a high chance of collision is there when guids are generated
    /// at high speeds. To prevent collisions, not the clockId is incremented (as recommended in
    /// RFC4122), but a new nodeId is generated.
    /// </remarks>
    public static class TimeGuid
    {
        private static readonly object SyncLock = new object();

        /// <summary>
        /// The default Time Guid (nodeId, time and sequence all set to 0, but having the version number set to timeguid)
        /// </summary>
        public static readonly Guid Default = GenerateTimeBasedGuid(0, 0, new byte[] {0, 0, 0, 0, 0, 0});

        // multiplex version info
        private const int VersionByte = 7;
        private const int VersionByteMask = 0x0f;
        private const int VersionByteShift = 4;

        // offset to move from 1/1/0001, which is 0-time for .NET, to gregorian 0-time of 10/15/1582
        public static readonly DateTime GregorianCalendarStart = new DateTime(1582, 10, 15, 0, 0, 0, DateTimeKind.Utc);

        private static readonly Random Random = new Random((int)DateTime.UtcNow.Ticks);

        // random node that is 16 bytes
        private static byte[] _nodeId;

        //clockId stuff
        private static readonly int ClockSequenceSeed;
        private const int MaxClockId = 1 << 14; //14 bits clockId
        private static int _clockSequenceNumber;

        //time stuff
        private static long _lastTime;
        private static int _timeSequenceNumber;

        /// <summary>
        /// Initializes the <see cref="TimeGuid" /> class.
        /// </summary>
        static TimeGuid()
        {
            _nodeId = CreateNodeId();
            ClockSequenceSeed = Random.Next()%MaxClockId;
            _clockSequenceNumber = (ClockSequenceSeed + 1)%MaxClockId;
        }

        /// <summary>
        /// Creates a node unique identifier.
        /// </summary>
        /// <returns></returns>
        private static byte[] CreateNodeId()
        {
            var nodeId = new byte[6];
            Random.NextBytes(nodeId);

            // turn on I/G and U/L bits (per the node spec)
            nodeId[0] |= 0xC0;

            return nodeId;
        }

        /// <summary>
        /// Gets the date time from the provided Guid.
        /// </summary>
        /// <param name="guid">The unique identifier.</param>
        /// <returns></returns>
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

        /// <summary>
        /// Generates a time based unique identifier, set to the current time.
        /// </summary>
        /// <returns></returns>
        public static Guid GenerateTimeBasedGuid()
        {
            return GenerateTimeBasedGuid(DateTime.UtcNow);
        }

        /// <summary>
        /// Generates a time based unique identifier.
        /// </summary>
        /// <param name="dateTime">The date time.</param>
        /// <param name="node">
        /// The node. Must either be null or a 6 byte array. When set to null (recommended), a random node id
        /// will be provided
        /// </param>
        /// <returns></returns>
        public static Guid GenerateTimeBasedGuid(this DateTime dateTime, byte[] node = null)
        {
            if(node != null && node.Length != 6)
                throw new ArgumentException("Node must either be null or a byte[] of length 6", "node");

            //get the 100ns since calendar start
            dateTime = dateTime.ToUniversalTime();
            var time = (dateTime - GregorianCalendarStart).Ticks;

            int sequence;
            byte[] nodeId;

            lock(SyncLock)
            {
                //generate a new nodeId when clock is set backward (to avoid collisions with earlier created uuids)
                if(time < _lastTime)
                    _nodeId = CreateNodeId();

                //if time changed, reset sequence numbers
                if(time != _lastTime)
                {
                    _clockSequenceNumber = ClockSequenceSeed;
                    _timeSequenceNumber = 0;
                }
                else
                {
                    //increment time if we are out of clockIds
                    if(_clockSequenceNumber == ClockSequenceSeed)
                        _timeSequenceNumber = _timeSequenceNumber + 1;

                    //increment clockId
                    _clockSequenceNumber = (_clockSequenceNumber + 1)%MaxClockId;
                }

                //cache this time
                _lastTime = time;

                //capture values
                time = time + _timeSequenceNumber;
                sequence = _clockSequenceNumber;
                nodeId = node ?? _nodeId;
            }

            return GenerateTimeBasedGuid(time, sequence, nodeId);
        }

        /// <summary>
        /// Generates a time based unique identifier.
        /// </summary>
        /// <param name="time">The time, being the number of 100ns intervals since Gregorian Calendar start</param>
        /// <param name="sequence">The sequence, or clockId.</param>
        /// <param name="nodeId">The node unique identifier.</param>
        /// <returns>a version 1 Guid as documented in RFC4122</returns>
        public static Guid GenerateTimeBasedGuid(long time, int sequence, byte[] nodeId)
        {
            var timeLow = (int)(time & 0xFFFFFFFF);
            var timeMid = (short)((time >> 32) & 0xFFFF);
            var timeHiAndVersion = (short)(((time >> 48) & 0x0FFF) | (1 << 12));
            var seqLow = (byte)(sequence & 0xFF);
            var seqHiAndReserved = (byte)(((sequence & 0x3F00) >> 8) | 0x80);

            var guid = new Guid(timeLow, timeMid, timeHiAndVersion, seqHiAndReserved, seqLow, nodeId[0], nodeId[1],
                                nodeId[2],
                                nodeId[3], nodeId[4], nodeId[5]);

            return guid;
        }
    }
}