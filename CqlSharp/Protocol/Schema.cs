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
        private readonly Lazy<IDictionary<string, Column>> _columnsByName;

        /// <summary>
        ///   Initializes a new instance of the <see cref="Schema" /> class.
        /// </summary>
        /// <param name="columns"> The columns. </param>
        internal Schema(IEnumerable<Column> columns)
        {
            _columns = columns as List<Column> ?? columns.ToList();
            _columnsByName = new Lazy<IDictionary<string, Column>>(() =>
                                                                          {
                                                                              var dict =
                                                                                  new Dictionary<string, Column>();

                                                                              foreach (Column column in _columns)
                                                                              {
                                                                                  dict[column.Name] = column;
                                                                                  dict[column.Table + "." + column.Name]
                                                                                      = column;
                                                                                  dict[
                                                                                      column.Keyspace + "." +
                                                                                      column.Table + "." + column.Name]
                                                                                      = column;
                                                                              }

                                                                              return dict;
                                                                          });
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
            get { return _columnsByName.Value[name]; }
            set { throw new NotSupportedException(); }
        }

        #region IList<Column> Members

        public int IndexOf(Column item)
        {
            return _columns.IndexOf(item);
        }

        public void Insert(int index, Column item)
        {
            throw new NotSupportedException();
        }

        public void RemoveAt(int index)
        {
            throw new NotSupportedException();
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
            set { throw new NotSupportedException(); }
        }

        public void Add(Column item)
        {
            throw new NotSupportedException();
        }

        public void Clear()
        {
            throw new NotSupportedException();
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
            get { return true; }
        }

        public bool Remove(Column item)
        {
            throw new NotSupportedException();
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