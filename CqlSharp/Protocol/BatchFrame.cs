using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace CqlSharp.Protocol
{
    /// <summary>
    /// Frame holding a batch of statements
    /// </summary>
    internal class BatchFrame : Frame
    {

        /// <summary>
        /// Structure to hold values of commands
        /// </summary>
        public class BatchedCommand
        {
            public bool IsPrepared { get; set; }
            public byte[] QueryId { get; set; }
            public string CqlQuery { get; set; }
            public byte[][] ParameterValues { get; set; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BatchFrame"/> class.
        /// </summary>
        /// <param name="version">The version.</param>
        public BatchFrame(FrameVersion version)
        {
            Debug.Assert((version & FrameVersion.ProtocolVersionMask) != FrameVersion.ProtocolVersion1, "Version 1 of the protocol does not support Batch Frames");

            Version = FrameVersion.Request | version;
            Flags = FrameFlags.None;
            Stream = 0;
            OpCode = FrameOpcode.Options;
        }

        /// <summary>
        /// Gets or sets the type of the batch.
        /// </summary>
        /// <value>
        /// The type.
        /// </value>
        public CqlBatchType Type { get; set; }

        /// <summary>
        /// Gets or sets the commands.
        /// </summary>
        /// <value>
        /// The commands.
        /// </value>
        public IList<BatchedCommand> Commands { get; set; }

        /// <summary>
        /// Gets or sets the CQL consistency.
        /// </summary>
        /// <value>
        /// The CQL consistency.
        /// </value>
        public CqlConsistency CqlConsistency { get; set; }

        /// <summary>
        /// Writes the data to buffer.
        /// </summary>
        /// <param name="buffer">The buffer.</param>
        protected override void WriteData(System.IO.Stream buffer)
        {
            buffer.WriteByte((byte)Type);
            buffer.WriteShort((ushort)Commands.Count);
            foreach (var command in Commands)
            {
                buffer.WriteByte(command.IsPrepared ? (byte)1 : (byte)0);
                if (command.IsPrepared)
                {
                    buffer.WriteByte(1);
                    buffer.WriteShortByteArray(command.QueryId);
                }
                else
                {
                    buffer.WriteByte(0);
                    buffer.WriteLongString(command.CqlQuery);
                }

                if (command.ParameterValues != null)
                {
                    var length = (ushort)command.ParameterValues.Length;
                    buffer.WriteShort(length);
                    for (var i = 0; i < length; i++)
                        buffer.WriteByteArray(command.ParameterValues[i]);
                }
                else
                {
                    buffer.WriteShort(0);
                }
            }

            buffer.WriteConsistency(CqlConsistency);

        }

        /// <summary>
        /// Initialize frame contents from the stream
        /// </summary>
        /// <returns></returns>
        /// <exception cref="System.NotSupportedException"></exception>
        protected override Task InitializeAsync()
        {
            throw new NotSupportedException();
        }
    }
}
