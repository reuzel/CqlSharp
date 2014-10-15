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

namespace CqlSharp.Protocol
{
    /// <summary>
    /// A description of a single Cql column. Used to describe input to a Cql prepared query, or result of a select query.
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
        /// Initializes a new instance of the <see cref="Column" /> class.
        /// </summary>
        /// <param name="index"> The index. </param>
        /// <param name="keyspace"> The keyspace. </param>
        /// <param name="table"> The table. </param>
        /// <param name="name"> The name. </param>
        /// <param name="type"> the type of the column </param>
        public Column(int index, string keyspace, string table, string name, CqlType type)
        {
            Index = index;
            Keyspace = keyspace;
            Table = table;
            Name = name;
            Type = type;
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

        public CqlType Type { get; set; }

        public string KeySpaceTableAndName
        {
            get
            {
                if(_ksTableName == null)
                {
                    _ksTableName = (Keyspace != null ? Keyspace + "." : string.Empty) +
                                   (Table != null ? Table + "." : string.Empty) +
                                   Name;
                }

                return _ksTableName;
            }
        }

        public string TableAndName
        {
            get
            {
                if(_tableName == null)
                    _tableName = (Table != null ? Table + "." : "") + Name;

                return _tableName;
            }
        }
    }
}