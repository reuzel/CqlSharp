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
using System.IO;
using System.Threading.Tasks;

namespace CqlSharp.Protocol
{
    /// <summary>
    /// Frame holding a batch of statements
    /// </summary>
    internal class BatchFrame : Frame
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BatchFrame" /> class.
        /// </summary>
        public BatchFrame(CqlBatchType batchType, CqlConsistency consistency)
        {
            IsRequest = true;
            Flags = FrameFlags.None;
            Stream = 0;
            OpCode = FrameOpcode.Batch;

            Type = batchType;
            CqlConsistency = consistency;
            Commands = new List<BatchedCommand>();
        }

        /// <summary>
        /// Gets or sets the type of the batch.
        /// </summary>
        /// <value> The type. </value>
        public CqlBatchType Type { get; set; }

        /// <summary>
        /// Gets or sets the commands.
        /// </summary>
        /// <value> The commands. </value>
        public IList<BatchedCommand> Commands { get; private set; }

        /// <summary>
        /// Gets or sets the CQL consistency.
        /// </summary>
        /// <value> The CQL consistency. </value>
        public CqlConsistency CqlConsistency { get; set; }

        /// <summary>
        /// The consistency level for the serial phase of conditional updates. That consitency
        /// can only be either SERIAL or LOCAL_SERIAL and if not present, it defaults to SERIAL.
        /// This option will be ignored for anything else that a conditional update/insert.
        /// </summary>
        /// <value> The serial consistency. </value>
        /// <exception cref="CqlException">Serial Consistency can only be LocalSerial or Serial</exception>
        public SerialConsistency? SerialConsistency { get; set; }

        /// <summary>
        /// The timestamp is a representing the default timestamp for the query. If provided, this will
        /// replace the server side assigned timestamp as default timestamp.
        /// </summary>
        /// <value>
        /// The (default) timestamp.
        /// </value>
        public DateTime? Timestamp { get; set; }

        /// <summary>
        /// Writes the data to buffer.
        /// </summary>
        /// <param name="buffer"> The buffer. </param>
        protected override void WriteData(Stream buffer)
        {
            if(ProtocolVersion == 1)
            {
                throw new ProtocolException(ProtocolVersion, ErrorCode.Protocol, "Batch frames are supported from Cassandra Version 2.0.0 and up.");
            }

            buffer.WriteByte((byte)Type);
            buffer.WriteShort((ushort)Commands.Count);
            foreach(var command in Commands)
            {
                if(command.IsPrepared)
                {
                    buffer.WriteByte(1);
                    buffer.WriteShortByteArray(command.QueryId);
                }
                else
                {
                    buffer.WriteByte(0);
                    buffer.WriteLongString(command.CqlQuery);
                }

                if(command.ParameterValues != null)
                {
                    byte[][] paramValues = command.ParameterValues.Serialize(ProtocolVersion);
                    var length = (ushort)paramValues.Length;
                    buffer.WriteShort(length);
                    for(var i = 0; i < length; i++)
                    {
                        buffer.WriteByteArray(paramValues[i]);
                    }
                }
                else
                    buffer.WriteShort(0);
            }

            buffer.WriteConsistency(CqlConsistency);

            if(ProtocolVersion >= 3)
            {
                var flags = (byte)((SerialConsistency.HasValue ? 16 : 0) |
                                   (Timestamp.HasValue ? 32 : 0));

                buffer.WriteByte(flags);

                if(SerialConsistency.HasValue)
                    buffer.WriteShort((ushort)SerialConsistency.Value);

                if(Timestamp.HasValue)
                    buffer.WriteLong(Timestamp.Value.ToTimestamp());
            }
        }

        /// <summary>
        /// Initialize frame contents from the stream
        /// </summary>
        /// <param name=""></param>
        /// <returns> </returns>
        /// <exception cref="System.NotSupportedException"></exception>
        protected override Task InitializeAsync()
        {
            throw new NotSupportedException();
        }

        #region Nested type: BatchedCommand

        /// <summary>
        /// Structure to hold values of commands
        /// </summary>
        public class BatchedCommand
        {
            public bool IsPrepared { get; set; }
            public byte[] QueryId { get; set; }
            public string CqlQuery { get; set; }
            public CqlParameterCollection ParameterValues { get; set; }
        }

        #endregion
    }
}