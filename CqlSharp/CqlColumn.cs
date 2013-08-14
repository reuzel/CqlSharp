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

namespace CqlSharp
{
    /// <summary>
    ///   A description of a single Cql column. Used to describe input to a Cql prepared query, or result of a select query.
    /// </summary>
    public class CqlColumn
    {
        private string _ksTableName;
        private string _tableName;

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlColumn" /> class.
        /// </summary>
        /// <param name="index"> The index. </param>
        /// <param name="keyspace"> The keyspace. </param>
        /// <param name="table"> The table. </param>
        /// <param name="name"> The name. </param>
        /// <param name="cqlType"> Type of the column. </param>
        /// <param name="customData"> The custom data. </param>
        /// <param name="collectionKeyType"> Type of the collection key. </param>
        /// <param name="collectionValueType"> Type of the collection value. </param>
        internal CqlColumn(int index, string keyspace, string table, string name, CqlType cqlType,
                           string customData, CqlType? collectionKeyType, CqlType? collectionValueType)
        {
            Index = index;
            Keyspace = keyspace;
            Table = table;
            Name = name;
            CqlType = cqlType;
            CustomData = customData;
            CollectionKeyType = collectionKeyType;
            CollectionValueType = collectionValueType;
        }

        public int Index { get; private set; }

        public string Keyspace { get; private set; }

        public string Table { get; private set; }

        public string Name { get; private set; }

        public CqlType CqlType { get; private set; }

        public string CustomData { get; private set; }

        public CqlType? CollectionKeyType { get; private set; }

        public CqlType? CollectionValueType { get; private set; }

        internal string KeySpaceTableAndName
        {
            get
            {
                if (_ksTableName == null)
                    _ksTableName = Keyspace + "." + Table + "." + Name;

                return _ksTableName;
            }
        }

        internal string TableAndName
        {
            get
            {
                if (_tableName == null)
                    _tableName = Table + "." + Name;

                return _tableName;
            }
        }

        public Type ToType()
        {
            return CqlType.ToType(CollectionKeyType, CollectionValueType);
        }
    }
}