// CqlSharp
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

namespace CqlSharp
{
    /// <summary>
    /// Set of options that controls the execution behaviour of a query
    /// </summary>
    [Flags]
    public enum ExecutionOptions
    {
        /// <summary>
        /// Default value, no execution options specified. Tracing is off, no buffering, and network connections are reused within a CqlConnection
        /// </summary>
        None = 0x00,

        /// <summary>
        /// Enables Cql tracing. The query result will contain a tracing id that can be used to query Cassandra for traces
        /// </summary>
        Tracing = 0x01,

        /// <summary>
        /// Enables response buffering. The entire response will be read into memory. This will more quickly free the connection for the
        /// receival of other responses at the cost of some memory.
        /// </summary>
        Buffering = 0x02,

        /// <summary>
        /// Signals that a command does not need to reuse the database network connection reserved by a CqlConnection, but may use another.
        /// This is useful if many queries are done in parallel within the scope of a single CqlConnection.
        /// </summary>
        ParallelConnection = 0x04
    }
}