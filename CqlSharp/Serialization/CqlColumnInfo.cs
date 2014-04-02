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
using System.Reflection;

namespace CqlSharp.Serialization
{
    /// <summary>
    ///   Provides CQL information of a table class property or field
    /// </summary>
    /// <typeparam name="TTable"> The type of the table. </typeparam>
    public class CqlColumnInfo<TTable> : ICqlColumnInfo
    {
        internal CqlColumnInfo()
        {
        }

        /// <summary>
        ///   Gets the column name.
        /// </summary>
        /// <value> The name. </value>
        public string Name { get; internal set; }

        /// <summary>
        ///   Gets the CQL type.
        /// </summary>
        /// <value> The type. </value>
        public CqlType CqlType { get; internal set; }

        /// <summary>
        ///   Gets the .NET type.
        /// </summary>
        /// <value> The type. </value>
        public Type Type { get; internal set; }

        /// <summary>
        ///   Gets the order/index of a key column.
        /// </summary>
        /// <value> The order. </value>
        public int? Order { get; internal set; }

        /// <summary>
        ///   Gets a value indicating whether this column is part of the partition key.
        /// </summary>
        /// <value> <c>true</c> if this column is part of the partition key; otherwise, <c>false</c> . </value>
        public bool IsPartitionKey { get; internal set; }


        /// <summary>
        ///   Gets a value indicating whether this column is part of the clustering key.
        /// </summary>
        /// <value> <c>true</c> if this column is part of the clustering key; otherwise, <c>false</c> . </value>
        public bool IsClusteringKey { get; internal set; }

        /// <summary>
        ///   Gets a value indicating whether this column is indexed.
        /// </summary>
        /// <value> <c>true</c> if this column is indexed; otherwise, <c>false</c> . </value>
        public bool IsIndexed { get; internal set; }

        /// <summary>
        ///   Gets the name of the index (if any).
        /// </summary>
        /// <value> The name of the index. </value>
        public string IndexName { get; internal set; }

        /// <summary>
        ///   Gets the member information.
        /// </summary>
        /// <value> The member information. </value>
        public MemberInfo MemberInfo { get; internal set; }

        /// <summary>
        ///   Gets the function that can be used to read this column value from a table object
        /// </summary>
        /// <value> The read function. </value>
        public Func<TTable, Object> ReadFunction { get; internal set; }

        /// <summary>
        ///   Gets the function that can be used to write a column value to a table object
        /// </summary>
        /// <value> The write function. </value>
        public Action<TTable, Object> WriteFunction { get; internal set; }

        /// <summary>
        /// Gets the function that can be used to write a column value to a table object
        /// </summary>
        /// <value>
        /// The write function.
        /// </value>
        Action<object, object> ICqlColumnInfo.WriteFunction
        {
            get { return WriteFunction == null ? default(Action<object, object>) : (table, obj) => WriteFunction((TTable)table, obj); }
        }

        /// <summary>
        /// Gets the function that can be used to read this column value from a table object
        /// </summary>
        /// <value>
        /// The read function.
        /// </value>
        Func<object, object> ICqlColumnInfo.ReadFunction
        {
            get { return ReadFunction == null ? default(Func<object, object>) : table => ReadFunction((TTable)table); }
        }
    }
}