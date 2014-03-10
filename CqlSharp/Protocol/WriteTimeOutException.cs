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
    ///   Timeout exception during a write request.
    /// </summary>
    [Serializable]
    public class WriteTimeOutException : TimeOutException
    {
        internal WriteTimeOutException(string message, CqlConsistency cqlConsistency, int received, int blockFor,
                                       string writeType, Guid? tracingId)
            : base(Protocol.ErrorCode.WriteTimeout, message, cqlConsistency, received, blockFor, tracingId)
        {
            WriteType = writeType;
        }

        /// <summary>
        /// Describes the type of the write that timeouted.
        /// </summary>
        /// <value>
        /// The type of the write. The value of that string can be one of: <list type="bullet">
        /// <listheader>
        /// <term>type</term>
        /// <description>description</description>
        /// </listheader>
        /// <item>
        /// <term>SIMPLE</term>
        /// <description>the write was a non-batched non-counter write.</description>
        /// </item>
        /// <item>
        /// <term>BATCH</term>
        /// <description>the write was a (logged) batch write. If this type is received, it means the batch log has been successfully written (otherwise a  "BATCH_LOG" type would have been send instead).</description>
        /// </item>
        /// <item>
        /// <term>UNLOGGED_BATCH</term>
        /// <description>the write was an unlogged batch. Not batch log write has been attempted.</description>
        /// </item>
        /// <item>
        /// <term>COUNTER</term>
        /// <description>the write was a counter write (batched or not).</description>
        /// </item>
        /// <item>
        /// <term>BATCH_LOG</term>
        /// <description>the timeout occured during the write to the batch log when a (logged) batch write was requested.</description>
        /// </item>
        /// </list>
        /// </value>
        public string WriteType { get; private set; }
    }
}