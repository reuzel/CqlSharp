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
    /// Thrown when authentication towards the Cassandra cluster fails
    /// </summary>
    [Serializable]
    public class AuthenticationException : ProtocolException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AuthenticationException" /> class.
        /// </summary>
        /// <param name="message">The message to display for this exception.</param>
        internal AuthenticationException(string message)
            : base(Protocol.ErrorCode.BadCredentials, message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AuthenticationException" /> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="tracingId">The tracing unique identifier.</param>
        internal AuthenticationException(string message, Guid? tracingId)
            : base(Protocol.ErrorCode.BadCredentials, message, tracingId)
        {
        }
    }
}