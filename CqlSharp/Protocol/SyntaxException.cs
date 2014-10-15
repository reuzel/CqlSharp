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
    /// Thrown when the submitted query has a syntax error
    /// </summary>
    [Serializable]
    public class SyntaxException : ProtocolException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SyntaxException"/> class.
        /// </summary>
        /// <param name="protocolVersion">The CQL binary protocol version in use.</param>
        /// <param name="message">The message.</param>
        /// <param name="tracingId">The tracing identifier.</param>
        internal SyntaxException(byte protocolVersion, string message, Guid? tracingId)
            : base(protocolVersion, Protocol.ErrorCode.Syntax, message, tracingId)
        {
        }
    }
}