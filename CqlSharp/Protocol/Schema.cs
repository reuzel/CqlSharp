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

using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace CqlSharp.Protocol
{
    /// <summary>
    ///   Represents a set of columns descriptions used to describe a select query result, or prepared query input
    /// </summary>
    internal class Schema : IList<Column>
    {
        /// <summary>
        ///   The columns
        /// </summary>
        private readonly IList<Column> _columns;

        /// <summary>
        ///   The columns by name. Lazy loaded
        /// </summary>
        private Dictionary<string, Column> _columnsByName;

        /// <summary>
        ///   Initializes a new instance of the <see cref="Schema" /> class.
        /// </summary>
        /// <param name="columns"> The columns. </param>
        internal Schema(IEnumerable<Column> columns)
        {
            _columns = columns as List<Column> ?? columns.ToList();
        }

        /// <summary>
        /// Rebuilds the dictionary with column name to column mappig
        /// </summary>
        private void RebuildNames()
        {
            if (_columnsByName != null)
                return;

            _columnsByName = new Dictionary<string, Column>();

            foreach (Column column in _columns)
            {
                _columnsByName[column.Name] = column;
                if (column.Table != null)
                    _columnsByName[column.TableAndName] = column;
                if (column.Keyspace != null)
                    _columnsByName[column.KeySpaceTableAndName] = column;
            }
        }

        /// <summary>
        /// Determines whether the schema holds a column with the given name
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns></returns>
        public bool Contains(string name)
        {
            RebuildNames();
            return _columnsByName.ContainsKey(name);
        }

        /// <summary>
        ///   Gets or sets the <see cref="Column" /> with the specified name. The column name is either
        ///   the {name}, {table}.{name}, or {keyspace}.{table}.{name}.
        /// </summary>
        /// <remarks>
        ///   if the name of the column occurs more often, it is undefined which column is returned.
        ///   In such cases, prepend the column name with the table and/or keyspace name to disambiguate.
        /// </remarks>
        /// <value> The <see cref="Column" /> . </value>
        /// <param name="name"> The name of the column </param>
        /// <returns> the column description </returns>
        /// <exception cref="System.NotSupportedException"></exception>
        public Column this[string name]
        {
            get
            {
                RebuildNames();
                return _columnsByName[name];
            }
            set
            {
                RebuildNames();

                Column c;
                if (_columnsByName.TryGetValue(name, out c))
                {
                    //existing value found, replace column at given index
                    _columns[c.Index] = value;
                    value.Index = c.Index;
                }
                else
                {
                    //no column matches the given name, add the column to the end
                    _columns.Add(value);
                    value.Index = _columns.Count - 1;
                }

                _columnsByName = null;
            }
        }

        /// <summary>
        /// Tries to get the column by name value.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="column">The column.</param>
        /// <returns></returns>
        public bool TryGetValue(string name, out Column column)
        {
            RebuildNames();
            return _columnsByName.TryGetValue(name, out column);
        }

        #region IList<Column> Members

        public int IndexOf(Column item)
        {
            return _columns.IndexOf(item);
        }

        public void Insert(int index, Column item)
        {
            _columns.Insert(index, item);

            //update indices
            for (int i = index; i < _columns.Count; i++)
                _columns[i].Index = i;

            //reset name map
            _columnsByName = null;
        }

        public void RemoveAt(int index)
        {
            _columns.RemoveAt(index);

            //update indices
            for (int i = index; i < _columns.Count; i++)
                _columns[i].Index = i;

            //reset name map
            _columnsByName = null;
        }

        /// <summary>
        ///   Gets or sets the <see cref="Column" /> at the specified index.
        /// </summary>
        /// <value> The <see cref="Column" /> . </value>
        /// <param name="index"> The index. </param>
        /// <returns> </returns>
        /// <exception cref="System.NotSupportedException"></exception>
        public Column this[int index]
        {
            get { return _columns[index]; }
            set
            {
                _columns[index] = value;
                _columnsByName = null;
            }
        }

        public void Add(Column item)
        {
            _columns.Add(item);
            item.Index = _columns.Count - 1;

            _columnsByName = null;
        }

        public void Clear()
        {
            _columns.Clear();
            _columnsByName = null;
        }

        public bool Contains(Column item)
        {
            return _columns.Contains(item);
        }

        public void CopyTo(Column[] array, int arrayIndex)
        {
            _columns.CopyTo(array, arrayIndex);
        }

        public int Count
        {
            get { return _columns.Count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool Remove(Column item)
        {
            if (_columns.Remove(item))
            {
                _columnsByName = null;
                return true;
            }

            return false;
        }

        public IEnumerator<Column> GetEnumerator()
        {
            return _columns.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _columns.GetEnumerator();
        }

        #endregion


    }
}