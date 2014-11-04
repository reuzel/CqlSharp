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
    /// Represent an error as returned from the Cassandra cluster.
    /// </summary>
    [Serializable]
    public class ProtocolException : CqlException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ProtocolException"/> class.
        /// </summary>
        /// <param name="protocolVersion">The CQL binary protocol version in use.</param>
        /// <param name="code">The error code.</param>
        /// <param name="message">The message.</param>
        internal ProtocolException(byte protocolVersion, ErrorCode code, string message)
            : base(code + ": "+message)
        {
            Code = code;
            ProtocolVersion = protocolVersion;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ProtocolException"/> class.
        /// </summary>
        /// <param name="protocolVersion">The CQL binary protocol version in use.</param>
        /// <param name="code">The code.</param>
        /// <param name="message">The message.</param>
        /// <param name="tracingId">The tracing identifier.</param>
        internal ProtocolException(byte protocolVersion, ErrorCode code, string message, Guid? tracingId)
            : base(code + ": " + message)
        {
            Code = code;
            ProtocolVersion = protocolVersion;
            TracingId = tracingId;
        }

        /// <summary>
        /// Gets the code of the error.
        /// </summary>
        /// <value>
        /// The code.
        /// </value>
        public ErrorCode Code { get; private set; }

        /// <summary>
        /// Gets the protocol version.
        /// </summary>
        /// <value>
        /// The protocol version.
        /// </value>
        public byte ProtocolVersion { get; private set; }

        /// <summary>
        /// Gets the tracing unique identifier, pointing to the server-side trace of the request leading
        /// to this exception
        /// </summary>
        /// <value>
        /// The tracing unique identifier.
        /// </value>
        public Guid? TracingId { get; private set; }

        ///// <summary>
        ///// Returns a <see cref="System.String" /> that represents this instance.
        ///// </summary>
        ///// <returns>
        ///// A <see cref="System.String" /> that represents this instance.
        ///// </returns>
        //public override string ToString()
        //{
        //    return string.Format("code {0} : {1}", Code, base.ToString());
        //}
    }
}