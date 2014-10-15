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

namespace CqlSharp.Protocol
{
    /// <summary>
    /// thrown when the creation of an existing table or keyspace is attempted
    /// </summary>
    [Serializable]
    public class AlreadyExistsException : ProtocolException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AlreadyExistsException" /> class.
        /// </summary>
        /// <param name="protocolVersion">The version of the protocol in use</param>
        /// <param name="message"> The message. </param>
        /// <param name="keyspace"> The keyspace. </param>
        /// <param name="table"> The table. </param>
        /// <param name="tracingId"> The tracing unique identifier. </param>
        internal AlreadyExistsException(byte protocolVersion, string message, string keyspace, string table, Guid? tracingId)
            : base(protocolVersion, Protocol.ErrorCode.AlreadyExists, message, tracingId)
        {
            Keyspace = keyspace;
            Table = table;
        }

        /// <summary>
        /// Gets the keyspace.
        /// </summary>
        /// <value> The keyspace. </value>
        public string Keyspace { get; private set; }

        /// <summary>
        /// Gets the table.
        /// </summary>
        /// <value> The table. </value>
        public string Table { get; private set; }
    }
}