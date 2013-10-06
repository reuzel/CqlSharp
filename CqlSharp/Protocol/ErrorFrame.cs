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
using System.IO;
using System.Threading.Tasks;

namespace CqlSharp.Protocol
{
    internal class ErrorFrame : Frame
    {
        public ProtocolException Exception { get; private set; }

        protected override void WriteData(Stream buffer)
        {
            throw new NotSupportedException();
        }

        protected override async Task InitializeAsync()
        {
            FrameReader stream = Reader;
            var code = (ErrorCode) await stream.ReadIntAsync().ConfigureAwait(false);
            string msg = await stream.ReadStringAsync().ConfigureAwait(false);

            switch (code)
            {
                case ErrorCode.Unavailable:
                    {
                        var cl = (CqlConsistency) await stream.ReadShortAsync().ConfigureAwait(false);
                        int required = await stream.ReadIntAsync().ConfigureAwait(false);
                        int alive = await stream.ReadIntAsync().ConfigureAwait(false);
                        Exception = new UnavailableException(msg, cl, required, alive);
                        break;
                    }

                case ErrorCode.WriteTimeout:
                    {
                        var cl = (CqlConsistency) await stream.ReadShortAsync().ConfigureAwait(false);
                        int received = await stream.ReadIntAsync().ConfigureAwait(false);
                        int blockFor = await stream.ReadIntAsync().ConfigureAwait(false);
                        string writeType = await stream.ReadStringAsync().ConfigureAwait(false);
                        Exception = new WriteTimeOutException(msg, cl, received, blockFor, writeType);
                        break;
                    }

                case ErrorCode.ReadTimeout:
                    {
                        var cl = (CqlConsistency) await stream.ReadShortAsync().ConfigureAwait(false);
                        int received = await stream.ReadIntAsync().ConfigureAwait(false);
                        int blockFor = await stream.ReadIntAsync().ConfigureAwait(false);
                        bool dataPresent = 0 != await stream.ReadByteAsync().ConfigureAwait(false);
                        Exception = new ReadTimeOutException(msg, cl, received, blockFor, dataPresent);
                        break;
                    }

                case ErrorCode.Syntax:
                    Exception = new SyntaxException(msg);
                    break;

                case ErrorCode.BadCredentials:
                    Exception = new AuthenticationException(msg);
                    break;

                case ErrorCode.Unauthorized:
                    Exception = new UnauthorizedException(msg);
                    break;

                case ErrorCode.Invalid:
                    Exception = new InvalidException(msg);
                    break;

                case ErrorCode.AlreadyExists:
                    string keyspace = await stream.ReadStringAsync().ConfigureAwait(false);
                    string table = await stream.ReadStringAsync().ConfigureAwait(false);
                    Exception = new AlreadyExistsException(msg, keyspace, table);
                    break;

                case ErrorCode.Unprepared:
                    byte[] unknownId = await stream.ReadShortBytesAsync().ConfigureAwait(false);
                    Exception = new UnpreparedException(msg, unknownId);
                    break;

                default:
                    Exception = new ProtocolException(code, msg);
                    break;
            }
        }
    }
}