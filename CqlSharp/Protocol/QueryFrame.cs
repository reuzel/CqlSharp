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
    internal class QueryFrame : QueryFrameBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="QueryFrame" /> class.
        /// </summary>
        /// <param name="cql"> The CQL. </param>
        /// <param name="cqlConsistency"> The CQL consistency. </param>
        /// <param name="parameters"> The parameters. </param>
        public QueryFrame(string cql, CqlConsistency cqlConsistency, byte[][] parameters)
        {
            Cql = cql;
            CqlConsistency = cqlConsistency;

            Version = FrameVersion.Request;
            Flags = FrameFlags.None;
            Stream = 0;
            OpCode = FrameOpcode.Query;

            Parameters = parameters;
        }

        /// <summary>
        /// Gets or sets the CQL query string.
        /// </summary>
        /// <value> The CQL. </value>
        public string Cql { get; set; }

        protected override void WriteData(Stream buffer)
        {
            buffer.WriteLongString(Cql);

            if((Version & FrameVersion.ProtocolVersionMask) == FrameVersion.ProtocolVersion1)
                buffer.WriteConsistency(CqlConsistency);
            else
                WriteQueryParameters(buffer);
        }
    }
}