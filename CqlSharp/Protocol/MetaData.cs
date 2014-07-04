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

using System.Collections;
using System.Collections.Generic;
using System.Threading;

namespace CqlSharp.Protocol
{
    /// <summary>
    /// Represents a set of columns descriptions used to describe a select query result, or prepared query input
    /// </summary>
    internal class MetaData : IList<Column>
    {
        /// <summary>
        /// The columns
        /// </summary>
        private List<Column> _columns = new List<Column>();

        /// <summary>
        /// The columns by name. Lazy loaded
        /// </summary>
        private Dictionary<string, Column> _columnsByName;

        /// <summary>
        /// Gets or sets the state of the paging.
        /// </summary>
        /// <value> The state of the paging. </value>
        public byte[] PagingState { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether any columns are specified in this meta Data
        /// </summary>
        /// <value> <c>true</c> if [no meta data]; otherwise, <c>false</c> . </value>
        public bool NoMetaData
        {
            get { return _columns.Count == 0; }
        }

        /// <summary>
        /// Gets a value indicating whether there are more rows in this query than has been returned.
        /// </summary>
        /// <value> <c>true</c> if [has more rows]; otherwise, <c>false</c> . </value>
        public bool HasMoreRows
        {
            get { return PagingState != null; }
        }

        /// <summary>
        /// Gets or sets the <see cref="Column" /> with the specified name. The column name is either
        /// the {name}, {table}.{name}, or {keyspace}.{table}.{name}.
        /// </summary>
        /// <remarks>
        /// if the name of the column occurs more often, it is undefined which column is returned.
        /// In such cases, prepend the column name with the table and/or keyspace name to disambiguate.
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
                if(_columnsByName.TryGetValue(name, out c))
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

        #region IList<Column> Members

        public int IndexOf(Column item)
        {
            return _columns.IndexOf(item);
        }

        public void Insert(int index, Column item)
        {
            _columns.Insert(index, item);

            //update indices
            for(int i = index; i < _columns.Count; i++)
            {
                _columns[i].Index = i;
            }

            //reset name map
            _columnsByName = null;
        }

        public void RemoveAt(int index)
        {
            _columns.RemoveAt(index);

            //update indices
            for(int i = index; i < _columns.Count; i++)
            {
                _columns[i].Index = i;
            }

            //reset name map
            _columnsByName = null;
        }

        /// <summary>
        /// Gets or sets the <see cref="Column" /> at the specified index.
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
            if(_columns.Remove(item))
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

        /// <summary>
        /// Rebuilds the dictionary with column name to column mappig
        /// </summary>
        private void RebuildNames()
        {
            if(_columnsByName != null)
                return;

            var columnsByName = new Dictionary<string, Column>();

            foreach(Column column in _columns)
            {
                columnsByName[column.Name] = column;
                if(column.Table != null)
                    columnsByName[column.TableAndName] = column;
                if(column.Keyspace != null)
                    columnsByName[column.KeySpaceTableAndName] = column;
            }

            Interlocked.CompareExchange(ref _columnsByName, columnsByName, null);
        }

        /// <summary>
        /// Determines whether the ResultMetaData holds a column with the given name
        /// </summary>
        /// <param name="name"> The name. </param>
        /// <returns> </returns>
        public bool Contains(string name)
        {
            RebuildNames();
            return _columnsByName.ContainsKey(name);
        }

        /// <summary>
        /// Tries to get the column by name value.
        /// </summary>
        /// <param name="name"> The name. </param>
        /// <param name="column"> The column. </param>
        /// <returns> </returns>
        public bool TryGetValue(string name, out Column column)
        {
            RebuildNames();
            return _columnsByName.TryGetValue(name, out column);
        }

        /// <summary>
        /// Copies the columns from an other MetaData instance
        /// </summary>
        /// <param name="metaData"> The meta data. </param>
        internal void CopyColumnsFrom(MetaData metaData)
        {
            if(metaData != null)
                _columns = metaData._columns;
        }
    }
}