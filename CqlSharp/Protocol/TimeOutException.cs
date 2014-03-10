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

namespace CqlSharp.Protocol
{
    /// <summary>
    /// Thrown when a query could not be executed within the scheduled timeframe.
    /// </summary>
    [Serializable]
    public abstract class TimeOutException : ProtocolException
    {
        protected TimeOutException(ErrorCode code, string message, CqlConsistency cqlConsistency, int received,
                                   int blockFor, Guid? tracingId)
            : base(code, message, tracingId)
        {
            CqlConsistency = cqlConsistency;
            Received = received;
            BlockFor = blockFor;
        }

        /// <summary>
        /// Gets the consistency level of the query having triggered the exception.
        /// </summary>
        public CqlConsistency CqlConsistency { get; private set; }

        /// <summary>
        /// Gets the the number of nodes having acknowledged the request.
        /// </summary>
        public int Received { get; private set; }

        /// <summary>
        /// the number of replica whose acknowledgement is required to achieve the requested consistency level
        /// </summary>
        public int BlockFor { get; private set; }
    }
}