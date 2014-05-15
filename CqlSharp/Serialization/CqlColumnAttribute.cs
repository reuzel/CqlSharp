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

namespace CqlSharp.Serialization
{
    /// <summary>
    ///   Annotates a field or property to have it map to a specific column, and optionally table and keyspace
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class CqlColumnAttribute : Attribute
    {
        private readonly string _column;
        private CqlTypeCode? _cqlTypeCode;
        private int? _order;

        public CqlColumnAttribute()
        {
        }

        public CqlColumnAttribute(string column)
        {
            _column = column;
        }

        public CqlColumnAttribute(string column, CqlTypeCode typeCode)
        {
            _column = column;
            CqlTypeCode = typeCode;
        }

        /// <summary>
        ///   Gets the name of the column
        /// </summary>
        /// <value> The column. </value>
        public string Column
        {
            get { return _column; }
        }


        /// <summary>
        /// Gets a value indicating whether [CQL typeCode has value].
        /// </summary>
        /// <value>
        ///   <c>true</c> if [CQL typeCode has value]; otherwise, <c>false</c>.
        /// </value>
        internal bool CqlTypeHasValue
        {
            get { return _cqlTypeCode.HasValue; }
        }

        /// <summary>
        ///   Gets or sets the Cql typeCode of the column
        /// </summary>
        /// <value> The typeCode of the CQL. </value>
        public CqlTypeCode CqlTypeCode
        {
            get
            {
                if (!_cqlTypeCode.HasValue)
                    throw new CqlException("CqlTypeCode attribute property was not set");

                return _cqlTypeCode.Value;
            }

            set { _cqlTypeCode = value; }
        }

        /// <summary>
        /// Gets a value indicating whether [Order has value].
        /// </summary>
        /// <value>
        ///   <c>true</c> if [Order has value]; otherwise, <c>false</c>.
        /// </value>
        internal bool OrderHasValue
        {
            get { return _order.HasValue; }
        }

        /// <summary>
        /// Gets or sets the order.
        /// </summary>
        /// <value>
        /// The order.
        /// </value>
        public int Order
        {
            get
            {
                if (!_order.HasValue)
                    throw new CqlException("Order attribute property was not set");

                return _order.Value;
            }

            set { _order = value; }
        }

        /// <summary>
        ///   Gets or sets the index of column in the partition key.
        /// </summary>
        /// <value> The index of the partition key. </value>
        [Obsolete("Please use CqlKeyAttribute instead", true)]
        public int PartitionKeyIndex { get; set; }
    }
}