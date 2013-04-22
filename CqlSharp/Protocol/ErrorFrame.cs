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
            var code = (ErrorCode) await stream.ReadIntAsync();
            string msg = await stream.ReadStringAsync();

            switch (code)
            {
                case ErrorCode.Unavailable:
                    {
                        var cl = (CqlConsistency) await stream.ReadShortAsync();
                        int required = await stream.ReadIntAsync();
                        int alive = await stream.ReadIntAsync();
                        Exception = new UnavailableException(msg, cl, required, alive);
                        break;
                    }

                case ErrorCode.WriteTimeout:
                    {
                        var cl = (CqlConsistency) await stream.ReadShortAsync();
                        int received = await stream.ReadIntAsync();
                        int blockFor = await stream.ReadIntAsync();
                        string writeType = await stream.ReadStringAsync();
                        Exception = new WriteTimeOutException(msg, cl, received, blockFor, writeType);
                        break;
                    }

                case ErrorCode.ReadTimeout:
                    {
                        var cl = (CqlConsistency) await stream.ReadShortAsync();
                        int received = await stream.ReadIntAsync();
                        int blockFor = await stream.ReadIntAsync();
                        bool dataPresent = 0 != await stream.ReadByteAsync();
                        Exception = new ReadTimeOutException(msg, cl, received, blockFor, dataPresent);
                        break;
                    }

                case ErrorCode.Syntax:
                    Exception = new SyntaxException(msg);
                    break;

                case ErrorCode.Unauthorized:
                    Exception = new UnauthorizedException(msg);
                    break;

                case ErrorCode.Invalid:
                    Exception = new InvalidException(msg);
                    break;

                case ErrorCode.AlreadyExists:
                    string keyspace = await stream.ReadStringAsync();
                    string table = await stream.ReadStringAsync();
                    Exception = new AlreadyExistsException(msg, keyspace, table);
                    break;

                case ErrorCode.Unprepared:
                    byte[] unknownId = await stream.ReadShortBytesAsync();
                    Exception = new UnpreparedException(msg, unknownId);
                    break;

                default:
                    Exception = new ProtocolException(code, msg);
                    break;
            }
        }
    }
}