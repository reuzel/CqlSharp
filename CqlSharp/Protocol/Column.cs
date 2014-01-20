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

namespace CqlSharp.Protocol
{
    /// <summary>
    ///   A description of a single Cql column. Used to describe input to a Cql prepared query, or result of a select query.
    /// </summary>
    internal class Column
    {
        private string _keyspace;
        private string _ksTableName;
        private string _name;
        private string _table;
        private string _tableName;

        public Column()
        {
            _name = string.Empty;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="Column" /> class.
        /// </summary>
        /// <param name="index"> The index. </param>
        /// <param name="keyspace"> The keyspace. </param>
        /// <param name="table"> The table. </param>
        /// <param name="name"> The name. </param>
        /// <param name="cqlType"> CqlType of the column. </param>
        /// <param name="customData"> The custom data. </param>
        /// <param name="collectionKeyType"> CqlType of the collection key. </param>
        /// <param name="collectionValueType"> CqlType of the collection value. </param>
        internal Column(int index, string keyspace, string table, string name, CqlType cqlType,
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

        public int Index { get; set; }

        public string Keyspace
        {
            get { return _keyspace; }
            set
            {
                _keyspace = value;
                _ksTableName = null;
                _tableName = null;
            }
        }

        public string Table
        {
            get { return _table; }
            set
            {
                _table = value;
                _ksTableName = null;
                _tableName = null;
            }
        }

        public string Name
        {
            get { return _name; }
            set
            {
                _name = value;
                _ksTableName = null;
                _tableName = null;
            }
        }

        public CqlType CqlType { get; set; }

        public string CustomData { get; private set; }

        public CqlType? CollectionKeyType { get; set; }

        public CqlType? CollectionValueType { get; set; }

        internal string KeySpaceTableAndName
        {
            get
            {
                if (_ksTableName == null)
                {
                    _ksTableName = (Keyspace != null ? Keyspace + "." : string.Empty) +
                                   (Table != null ? Table + "." : string.Empty) +
                                   Name;
                }

                return _ksTableName;
            }
        }

        internal string TableAndName
        {
            get
            {
                if (_tableName == null)
                    _tableName = (Table != null ? Table + "." : "") + Name;

                return _tableName;
            }
        }

        /// <summary>
        ///   Returns the .NET type representing the column type
        /// </summary>
        /// <returns> </returns>
        public Type ToType()
        {
            return CqlType.ToType(CollectionKeyType, CollectionValueType);
        }

        /// <summary>
        ///   Guesses the type of the column from the .NET type
        /// </summary>
        /// <param name="type"> The type. </param>
        /// <exception cref="CqlException">Unsupported type</exception>
        public void GuessType(Type type)
        {
            CqlType cqlType;
            CqlType? keyType = null;
            CqlType? valueType = null;

            if (type.IsGenericType)
            {
                var genericType = type.GetGenericTypeDefinition();

                //check for collection types
                if (genericType == typeof (List<>))
                {
                    cqlType = CqlType.List;
                    valueType = type.GetGenericArguments()[0].ToCqlType();
                }
                else if (genericType == typeof (HashSet<>))
                {
                    cqlType = CqlType.Set;
                    valueType = type.GetGenericArguments()[0].ToCqlType();
                }
                else if (genericType == typeof (Dictionary<,>))
                {
                    cqlType = CqlType.Map;
                    keyType = type.GetGenericArguments()[0].ToCqlType();
                    valueType = type.GetGenericArguments()[1].ToCqlType();
                }
                else
                {
                    throw new CqlException("Unsupported type");
                }
            }
            else
            {
                cqlType = type.ToCqlType();
            }

            //all is well, set the type values
            CqlType = cqlType;
            CollectionKeyType = keyType;
            CollectionValueType = valueType;
        }
    }
}