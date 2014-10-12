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
    /// Thrown when not enough Cassandra nodes were available to execute the query.
    /// </summary>
    [Serializable]
    public class UnavailableException : ProtocolException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="UnavailableException"/> class.
        /// </summary>
        /// <param name="protocolVersion">The CQL binary protocol version in use.</param>
        /// <param name="message">The message.</param>
        /// <param name="cqlConsistency">The CQL consistency.</param>
        /// <param name="required">The required.</param>
        /// <param name="alive">The alive.</param>
        /// <param name="tracingId">The tracing identifier.</param>
        internal UnavailableException(byte protocolVersion, string message, CqlConsistency cqlConsistency, int required, int alive, Guid? tracingId)
            : base(protocolVersion, Protocol.ErrorCode.Unavailable, message, tracingId)
        {
            CqlConsistency = cqlConsistency;
            Required = required;
            Alive = alive;
        }

        /// <summary>
        /// Gets the CQL consistency level of the query having triggered the exception
        /// </summary>
        /// <value>
        /// The CQL consistency.
        /// </value>
        public CqlConsistency CqlConsistency { get; private set; }

        /// <summary>
        /// Gets the number of nodes that should be alive to respect the requested consistency level.
        /// </summary>
        /// <value>
        /// The required number of nodes.
        /// </value>
        public int Required { get; private set; }

        /// <summary>
        /// Gets the number of replicas that were known to be alive when the request has been processed
        /// </summary>
        /// <value>
        /// the number of replicas alive. Alive &lt; Required.
        /// </value>
        public int Alive { get; private set; }
    }
}