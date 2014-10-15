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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using CqlSharp.Memory;
using CqlSharp.Network.nSnappy;
using CqlSharp.Threading;

namespace CqlSharp.Protocol
{
    /// <summary>
    /// A Cassandra protocol data packet
    /// </summary>
    internal abstract class Frame : IDisposable
    {
        /// <summary>
        /// Stream holding Frame content
        /// </summary>
        protected FrameReader Reader;

        private int _disposed; //0 not disposed, 1 disposed

        /// <summary>
        /// Gets or sets the protocol version.
        /// </summary>
        /// <value> The version. </value>
        public byte ProtocolVersion { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this instance is a request frame.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is request; otherwise, <c>false</c>.
        /// </value>
        public bool IsRequest { get; set; }

        /// <summary>
        /// Gets or sets the flags.
        /// </summary>
        /// <value> The flags. </value>
        public FrameFlags Flags { get; set; }

        /// <summary>
        /// Gets or sets the stream identifier (request response pair)
        /// </summary>
        /// <value> The stream. </value>
        public short Stream { get; set; }

        /// <summary>
        /// Gets or sets the op code.
        /// </summary>
        /// <value> The op code. </value>
        public FrameOpcode OpCode { get; protected set; }


        /// <summary>
        /// Gets or sets the length.
        /// </summary>
        /// <value> The length. </value>
        public int Length { get; protected set; }

        /// <summary>
        /// Gets or sets the tracing id.
        /// </summary>
        /// <value> The tracing id. </value>
        public Guid? TracingId { get; protected set; }

        #region IDisposable Members

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        /// <summary>
        /// Gets the frame bytes.
        /// </summary>
        /// <returns> </returns>
        public PoolMemoryStream GetFrameBytes(bool compress, int compressTreshold)
        {
            var buffer = new PoolMemoryStream();

            int versionByte = (ProtocolVersion & 0x7f) | (IsRequest ? 0 : 0x80);
            buffer.WriteByte((byte)versionByte);
            buffer.WriteByte((byte)Flags);
            if(ProtocolVersion <= 2)
                buffer.WriteByte(unchecked((byte)Stream));
            else
                buffer.WriteShort(unchecked((ushort)Stream));
            buffer.WriteByte((byte)OpCode);

            //write length placeholder
            buffer.WriteInt(0);

            long headerLength = buffer.Position;

            //write uncompressed data
            WriteData(buffer);

            //compress if allowed, and buffer is large enough to compress
            if(compress && buffer.Length > headerLength + compressTreshold)
            {
                //rewind to start of content
                buffer.Position = headerLength;

                //compress data to temporary stream
                using(var compressed = new PoolMemoryStream())
                {
                    //compress the data to the buffer
                    int length = Compressor.Compress(buffer, compressed);

                    //add compression to flags
                    Flags |= FrameFlags.Compression;
                    buffer.Position = 1;
                    buffer.WriteByte((byte)Flags);

                    //overwrite data with compressed data
                    buffer.Position = headerLength;
                    compressed.Position = 0;
                    compressed.CopyTo(buffer);
                    buffer.SetLength(headerLength + length);
                }
            }

            //overwrite length with real value
            buffer.Position = headerLength - 4;
            buffer.WriteInt((int)(buffer.Length - headerLength));

            //reset buffer position
            buffer.Position = 0;

            //return the buffer
            return buffer;
        }

        /// <summary>
        /// Writes the data to buffer.
        /// </summary>
        /// <param name="buffer"> The buffer. </param>
        protected abstract void WriteData(Stream buffer);

        /// <summary>
        /// Reads a packet from a stream.
        /// </summary>
        /// <param name="stream"> The stream. </param>
        /// <returns> </returns>
        internal static async Task<Frame> FromStream(Stream stream)
        {
            int read = 0;
            var header = new byte[9];

            //read version byte first
            if(Scheduler.RunningSynchronously)
                read += stream.Read(header, 0, 1);
            else
                read += await stream.ReadAsync(header, 0, 1).AutoConfigureAwait();

            //return null if the stream was closed by Cassandra
            if(read == 0)
                return null;

            //distill version
            var protocolVersion = (byte)(header[0] & 0x7f);

            //read remaining header bytes
            int headerSize = protocolVersion <= 2 ? 8 : 9;
            while(read < headerSize)
            {
                if(Scheduler.RunningSynchronously)
                    read += stream.Read(header, read, headerSize - read);
                else
                    read += await stream.ReadAsync(header, read, headerSize - read).AutoConfigureAwait();

                //check if we are not end-of-stream
                if(read == 0 && read < headerSize)
                    throw new IOException("Unexpected end of stream reached");
            }

            Frame frame;
            if(protocolVersion <= 2)
            {
                var opcode = (FrameOpcode)header[3];
                frame = GetFrameFromOpcode(protocolVersion, opcode);

                frame.ProtocolVersion = protocolVersion;
                frame.IsRequest = (header[0] & 0x80) == 0;
                frame.Flags = (FrameFlags)header[1];
                frame.Stream = unchecked((sbyte)header[2]);
                frame.OpCode = opcode;
                frame.Length = header.ToInt(4);
            }
            else
            {
                var opcode = (FrameOpcode)header[4];
                frame = GetFrameFromOpcode(protocolVersion, opcode);

                frame.ProtocolVersion = protocolVersion;
                frame.IsRequest = (header[0] & 0x80) == 0;
                frame.Flags = (FrameFlags)header[1];
                frame.Stream = unchecked((short)header.ToShort(2));
                frame.OpCode = opcode;
                frame.Length = header.ToInt(5);
            }

            //wrap the stream in a window, that will be completely read when disposed
            var reader = new FrameReader(stream, frame.Length);
            frame.Reader = reader;

            return frame;
        }

        /// <summary>
        /// Gets the frame from opcode.
        /// </summary>
        /// <param name="protocolVersion">the version of the cql binary protocol in use</param>
        /// <param name="opcode">The opcode.</param>
        /// <returns></returns>
        /// <exception cref="ProtocolException">Unexpected OpCode received.</exception>
        private static Frame GetFrameFromOpcode(byte protocolVersion, FrameOpcode opcode)
        {
            Frame frame;
            switch(opcode)
            {
                case FrameOpcode.Error:
                    frame = new ErrorFrame();
                    break;
                case FrameOpcode.Ready:
                    frame = new ReadyFrame();
                    break;
                case FrameOpcode.Authenticate:
                    frame = new AuthenticateFrame();
                    break;
                case FrameOpcode.AuthChallenge:
                    frame = new AuthChallengeFrame();
                    break;
                case FrameOpcode.AuthSuccess:
                    frame = new AuthSuccessFrame();
                    break;
                case FrameOpcode.Supported:
                    frame = new SupportedFrame();
                    break;
                case FrameOpcode.Result:
                    frame = new ResultFrame();
                    break;
                case FrameOpcode.Event:
                    frame = new EventFrame();
                    break;
                default:
                    throw new ProtocolException(protocolVersion, 0, string.Format("Unexpected OpCode {0:X} received.", opcode));
            }
            return frame;
        }

        /// <summary>
        /// Reads the frame content asynchronous.
        /// </summary>
        /// <returns></returns>
        internal Task ReadFrameContentAsync()
        {
            //decompress the contents of the frame (implicity loads the entire frame body!)
            if(!Flags.HasFlag(FrameFlags.Compression) && !Flags.HasFlag(FrameFlags.Tracing))
                return InitializeAsync();

            return PrepareAndInitializeContentAsync();
        }

        /// <summary>
        /// Prepares the reader/content and initializes content asynchronous.
        /// </summary>
        /// <returns></returns>
        private async Task PrepareAndInitializeContentAsync()
        {
            //decompress the contents of the frame (implicity loads the entire frame body!)
            if(Flags.HasFlag(FrameFlags.Compression))
                await Reader.DecompressAsync().AutoConfigureAwait();

            //read tracing id if set
            if(Flags.HasFlag(FrameFlags.Tracing))
                TracingId = await Reader.ReadUuidAsync().AutoConfigureAwait();

            await InitializeAsync().AutoConfigureAwait();
        }

        /// <summary>
        /// Initialize frame contents from the stream
        /// </summary>
        protected abstract Task InitializeAsync();

        /// <summary>
        /// Completes when the frame body is read
        /// </summary>
        /// <returns> </returns>
        public virtual Task WaitOnBodyRead()
        {
            return Reader.WaitUntilFrameDataRead;
        }


        protected virtual void Dispose(bool disposing)
        {
            if(Interlocked.CompareExchange(ref _disposed, 1, 0) == 0)
            {
                if(disposing)
                {
                    Reader.Dispose();
                    //Reader = null;
                }
            }
        }

        ~Frame()
        {
            Dispose(false);
        }
    }
}