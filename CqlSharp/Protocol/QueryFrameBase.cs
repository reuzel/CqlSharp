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
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using CqlSharp.Threading;

namespace CqlSharp.Protocol
{
    internal abstract class QueryFrameBase : Frame
    {
        /// <summary>
        ///   Gets or sets the CQL consistency.
        /// </summary>
        /// <value> The CQL consistency level for the operation. </value>
        public CqlConsistency CqlConsistency { get; set; }

        /// <summary>
        ///   Gets or sets the parameters.
        /// </summary>
        /// <value> The parameter values used for bound variables in the query. </value>
        public IList<byte[]> Parameters { get; set; }

        /// <summary>
        ///   Gets or sets a value indicating whether to skip meta data. If set, the Result Set returned as a response to that query (if any) will have the NO_METADATA flag
        /// </summary>
        /// <value> <c>true</c> if [skip meta data]; otherwise, <c>false</c> . </value>
        public bool SkipMetaData { get; set; }

        /// <summary>
        ///   Gets or sets the size of the page.
        /// </summary>
        /// <value> an int controlling the desired page size of the result (in CQL3 rows). </value>
        public int? PageSize { get; set; }

        /// <summary>
        ///   If provided, the query will be executed but starting from a given paging state.
        /// </summary>
        /// <value> The state of the paging. </value>
        public byte[] PagingState { get; set; }

        /// <summary>
        ///   The consistency level for the serial phase of conditional updates. That consitency
        ///   can only be either SERIAL or LOCAL_SERIAL and if not present, it defaults to SERIAL.
        ///   This option will be ignored for anything else that a conditional update/insert.
        /// </summary>
        /// <value> The serial consistency. </value>
        /// <exception cref="CqlException">Serial Consistency can only be LocalSerial or Serial</exception>
        public SerialConsistency? SerialConsistency { get; set; }

        /// <summary>
        /// Writes the query parameters.
        /// </summary>
        /// <param name="buffer">The buffer.</param>
        protected void WriteQueryParameters(Stream buffer)
        {
            buffer.WriteConsistency(CqlConsistency);

            var flags = (byte)((Parameters != null ? 1 : 0) |
                                (SkipMetaData ? 2 : 0) |
                                (PageSize.HasValue ? 4 : 0) |
                                (PagingState != null ? 8 : 0) |
                                (SerialConsistency.HasValue ? 16 : 0));

            buffer.WriteByte(flags);

            if (Parameters != null)
            {
                buffer.WriteShort((ushort)Parameters.Count);
                foreach (var value in Parameters)
                    buffer.WriteByteArray(value);
            }

            if (PageSize.HasValue)
            {
                buffer.WriteInt(PageSize.Value);
            }

            if (PagingState != null)
            {
                buffer.WriteByteArray(PagingState);
            }

            if (SerialConsistency.HasValue)
            {
                buffer.WriteShort((ushort)SerialConsistency.Value);
            }
        }

        internal override Task InitializeAsync()
        {
            throw new NotSupportedException();
        }
    }
}