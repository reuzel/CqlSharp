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
using CqlSharp.Threading;

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
        /// Writes the data to buffer.
        /// </summary>
        /// <param name="buffer"> The buffer. </param>
        protected override void WriteData(Stream buffer)
        {
            if(ProtocolVersion == 1)
            {
                throw new ProtocolException(ErrorCode.Protocol,
                                            "Batch frames are supported from Cassandra Version 2.0.0 and up.", null);
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