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
    ///   Indicates that this column is part of the (partition/clustering) key of the table
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class CqlKeyAttribute : Attribute
    {
        private bool? _isPartitionKey;

        public CqlKeyAttribute()
        {
            Order = 0;
            _isPartitionKey = null;
        }

        /// <summary>
        ///   Gets or sets the order.
        /// </summary>
        /// <value> The order. </value>
        public int Order { get; set; }

        /// <summary>
        ///   Gets or sets a value indicating whether this key value is part of the partition key.
        /// </summary>
        /// <value> <c>true</c> if [is partition key]; otherwise, <c>false</c> . </value>
        public bool IsPartitionKey
        {
            get
            {
                if (!_isPartitionKey.HasValue)
                {
                    return Order == 0;
                }

                return _isPartitionKey.Value;
            }
            set { _isPartitionKey = value; }
        }
    }
}