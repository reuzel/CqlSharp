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

namespace CqlSharp.Serialization
{
    /// <summary>
    ///   Annotates a field or property to have it map to a specific column, and optionally table and keyspace
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class CqlColumnAttribute : Attribute
    {
        private readonly string _column;

        public CqlColumnAttribute(string column)
        {
            _column = column;
        }

        /// <summary>
        /// Gets the name of the column
        /// </summary>
        /// <value>
        /// The column.
        /// </value>
        public string Column
        {
            get { return _column; }
        }

        /// <summary>
        /// Gets or sets the Cql type of the column (only necessary if it is used as PartitionKey)
        /// </summary>
        /// <value>
        /// The type of the CQL.
        /// </value>
        public CqlType CqlType { get; set; }

        /// <summary>
        /// Gets or sets the index of column in the partition key.
        /// </summary>
        /// <value>
        /// The index of the partition key.
        /// </value>
        public int PartitionKeyIndex { get; set; }
    }
}