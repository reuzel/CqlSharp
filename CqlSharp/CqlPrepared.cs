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

namespace CqlSharp
{
    /// <summary>
    ///   Represents the result of a query that does not have actual result values
    /// </summary>
    public class CqlPrepared : ICqlQueryResult
    {
        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlPrepared" /> class.
        /// </summary>
        /// <param name="fromCache"> if set to <c>true</c> the query was prepared from a cache, without a roundtrip to the Cassandra cluster. </param>
        /// <param name="tracingId"> The tracing unique identifier. </param>
        internal CqlPrepared(bool fromCache, Guid? tracingId)
        {
            FromCache = fromCache;
            TracingId = tracingId;
        }

        /// <summary>
        /// Gets a value indicating whether the query was prepared from a cache, without a roundtrip to the Cassandra cluster..
        /// </summary>
        /// <value>
        ///   <c>true</c> if the query was prepared from a cache, without a roundtrip to the Cassandra cluster.; otherwise, <c>false</c>.
        /// </value>
        public bool FromCache { get; private set; }

        #region ICqlQueryResult Members

        /// <summary>
        /// Gets the type of the result.
        /// </summary>
        /// <value>
        /// The type of the result.
        /// </value>
        public CqlResultType ResultType
        {
            get { return CqlResultType.Prepared; }
        }

        /// <summary>
        /// Gets the tracing id, if present
        /// </summary>
        /// <value>
        /// The tracing id, if present
        /// </value>
        public Guid? TracingId { get; private set; }

        #endregion
    }
}