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

using System.IO;

namespace CqlSharp.Protocol
{
    internal class ExecuteFrame : QueryFrameBase
    {
        public ExecuteFrame(byte[] queryId, CqlConsistency cqlConsistency, byte[][] parameters)
        {
            QueryId = queryId;
            CqlConsistency = cqlConsistency;
            Parameters = parameters;
            SkipMetaData = true;

            IsRequest = true;
            Flags = FrameFlags.None;
            Stream = 0;
            OpCode = FrameOpcode.Execute;
        }

        public byte[] QueryId { get; set; }

        protected override void WriteData(Stream buffer)
        {
            buffer.WriteShortByteArray(QueryId);

            if(ProtocolVersion == 1)
            {
                if(Parameters == null)
                    buffer.WriteShort(0);
                else
                {
                    buffer.WriteShort((ushort)Parameters.Count);
                    foreach(var prm in Parameters)
                        buffer.WriteByteArray(prm);
                }

                buffer.WriteConsistency(CqlConsistency);
            }
            else
                WriteQueryParameters(buffer);
        }
    }
}