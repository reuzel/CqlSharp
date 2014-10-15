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
using System.Collections.ObjectModel;
using System.Reflection;
using CqlSharp.Network.Partition;

namespace CqlSharp.Serialization
{
    /// <summary>
    /// Provides non-generic access to an ObjectAccessor instance
    /// </summary>
    public interface IObjectAccessor
    {
        /// <summary>
        /// Gets a value indicating whether [is key space set].
        /// </summary>
        /// <value> <c>true</c> if [is key space set]; otherwise, <c>false</c> . </value>
        bool IsKeySpaceSet { get; }

        /// <summary>
        /// Gets the keyspace.
        /// </summary>
        /// <value> The keyspace. </value>
        string Keyspace { get; }

        /// <summary>
        /// Gets a value indicating whether [is table set].
        /// </summary>
        /// <value> <c>true</c> if [is table set]; otherwise, <c>false</c> . </value>
        bool IsNameSet { get; }

        /// <summary>
        /// Gets the name of the entity (e.g. table).
        /// </summary>
        /// <value> The name. </value>
        string Name { get; }

        /// <summary>
        /// Gets the type this accessor can handle
        /// </summary>
        /// <value> The type. </value>
        Type Type { get; }

        /// <summary>
        /// Gets the partition keys.
        /// </summary>
        /// <value> The partition keys. </value>
        ReadOnlyCollection<ICqlColumnInfo> PartitionKeys { get; }

        /// <summary>
        /// Gets the clustering keys.
        /// </summary>
        /// <value> The clustering keys. </value>
        ReadOnlyCollection<ICqlColumnInfo> ClusteringKeys { get; }

        /// <summary>
        /// Gets the normal (non-key) columns.
        /// </summary>
        /// <value> The normal columns. </value>
        ReadOnlyCollection<ICqlColumnInfo> NormalColumns { get; }

        /// <summary>
        /// Gets all the columns.
        /// </summary>
        /// <value> The columns. </value>
        ReadOnlyCollection<ICqlColumnInfo> Columns { get; }

        /// <summary>
        /// Gets the columns by field or property member.
        /// </summary>
        /// <value> The columns by member. </value>
        ReadOnlyDictionary<MemberInfo, ICqlColumnInfo> ColumnsByMember { get; }

        /// <summary>
        /// Gets the columns by column name. When the Table or Keyspace is known the dictionary
        /// will contain entries where the column name is combined with the Table or Keyspace names.
        /// </summary>
        /// <value> The columns by member. </value>
        ReadOnlyDictionary<string, ICqlColumnInfo> ColumnsByName { get; }

        /// <summary>
        /// Tries to get a value from the source, based on the column description
        /// </summary>
        /// <param name="columnName"> Name of the column. </param>
        /// <param name="source"> The source. </param>
        /// <param name="value"> The value. </param>
        /// <returns> true, if the value could be distilled from the source </returns>
        /// <exception cref="System.ArgumentNullException">columnName or source are null</exception>
        bool TryGetValue(string columnName, object source, out object value);

        /// <summary>
        /// Tries to set a property or field of the specified object, based on the column description
        /// </summary>
        /// <param name="columnName"> Name of the column. </param>
        /// <param name="target"> The target. </param>
        /// <param name="value"> The value. </param>
        /// <returns> true if the property or field value is set </returns>
        /// <exception cref="System.ArgumentNullException">columnName or target are null</exception>
        bool TrySetValue(string columnName, object target, object value);

        /// <summary>
        /// Sets the partition key based on the data found in a table entry.
        /// </summary>
        /// <param name="key"> The key. </param>
        /// <param name="value"> The value. </param>
        /// <exception cref="CqlException">Unable to read value for partition key column  + _partitionKeys[i].Name</exception>
        void SetPartitionKey(PartitionKey key, object value);
    }
}