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
using System.Threading.Tasks;
using CqlSharp.Threading;

namespace CqlSharp.Protocol
{
    /// <summary>
    /// an error frame as returned from the Cassandra cluster
    /// </summary>
    internal class ErrorFrame : Frame
    {
        /// <summary>
        /// Gets the exception representing the error.
        /// </summary>
        /// <value>
        /// The exception.
        /// </value>
        public ProtocolException Exception { get; private set; }

        /// <summary>
        /// Writes the data to buffer.
        /// </summary>
        /// <param name="buffer">The buffer.</param>
        /// <exception cref="System.NotSupportedException"></exception>
        protected override void WriteData(Stream buffer)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Initialize frame contents from the stream
        /// </summary>
        /// <param name=""></param>
        /// <returns></returns>
        protected override async Task InitializeAsync()
        {
            FrameReader stream = Reader;
            var code = (ErrorCode)await stream.ReadIntAsync().AutoConfigureAwait();
            string msg = await stream.ReadStringAsync().AutoConfigureAwait();

            switch(code)
            {
                case ErrorCode.Unavailable:
                {
                    var cl = (CqlConsistency)await stream.ReadShortAsync().AutoConfigureAwait();
                    int required = await stream.ReadIntAsync().AutoConfigureAwait();
                    int alive = await stream.ReadIntAsync().AutoConfigureAwait();
                    Exception = new UnavailableException(msg, cl, required, alive, TracingId);
                    break;
                }

                case ErrorCode.WriteTimeout:
                {
                    var cl = (CqlConsistency)await stream.ReadShortAsync().AutoConfigureAwait();
                    int received = await stream.ReadIntAsync().AutoConfigureAwait();
                    int blockFor = await stream.ReadIntAsync().AutoConfigureAwait();
                    string writeType = await stream.ReadStringAsync().AutoConfigureAwait();
                    Exception = new WriteTimeOutException(msg, cl, received, blockFor, writeType, TracingId);
                    break;
                }

                case ErrorCode.ReadTimeout:
                {
                    var cl = (CqlConsistency)await stream.ReadShortAsync().AutoConfigureAwait();
                    int received = await stream.ReadIntAsync().AutoConfigureAwait();
                    int blockFor = await stream.ReadIntAsync().AutoConfigureAwait();
                    bool dataPresent = 0 != await stream.ReadByteAsync().AutoConfigureAwait();
                    Exception = new ReadTimeOutException(msg, cl, received, blockFor, dataPresent, TracingId);
                    break;
                }

                case ErrorCode.Syntax:
                    Exception = new SyntaxException(msg, TracingId);
                    break;

                case ErrorCode.BadCredentials:
                    Exception = new AuthenticationException(msg, TracingId);
                    break;

                case ErrorCode.Unauthorized:
                    Exception = new UnauthorizedException(msg, TracingId);
                    break;

                case ErrorCode.Invalid:
                    Exception = new InvalidException(msg, TracingId);
                    break;

                case ErrorCode.AlreadyExists:
                    string keyspace = await stream.ReadStringAsync().AutoConfigureAwait();
                    string table = await stream.ReadStringAsync().AutoConfigureAwait();
                    Exception = new AlreadyExistsException(msg, keyspace, table, TracingId);
                    break;

                case ErrorCode.Unprepared:
                    byte[] unknownId = await stream.ReadShortBytesAsync().AutoConfigureAwait();
                    Exception = new UnpreparedException(msg, unknownId, TracingId);
                    break;

                default:
                    Exception = new ProtocolException(code, msg, TracingId);
                    break;
            }
        }
    }
}