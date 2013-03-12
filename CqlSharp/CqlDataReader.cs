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
using System.Threading;
using System.Threading.Tasks;
using CqlSharp.Protocol;
using CqlSharp.Protocol.Frames;
using CqlSharp.Serialization;

namespace CqlSharp
{
    /// <summary>
    ///   Provides access to a set of Cql data rows as returned from a query
    /// </summary>
    public class CqlDataReader : ICqlQueryResult, IDisposable
    {
        private readonly ResultFrame _frame;
        protected byte[][] CurrentValues;
        private int _disposed;

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlDataReader" /> class.
        /// </summary>
        /// <param name="frame"> The frame. </param>
        internal CqlDataReader(ResultFrame frame)
        {
            _frame = frame;
        }

        /// <summary>
        ///   Gets the schema.
        /// </summary>
        /// <value> The schema. </value>
        public CqlSchema Schema
        {
            get { return _frame.Schema; }
        }

        /// <summary>
        ///   Gets a value indicating whether this reader instance has more rows.
        /// </summary>
        /// <value> <c>true</c> if this instance has more rows; otherwise, <c>false</c> . </value>
        public bool HasRows
        {
            get { return _frame.Count > 0; }
        }

        /// <summary>
        ///   Gets the amount of results that can be read from this DataReader
        /// </summary>
        /// <value> The count. </value>
        public int Count
        {
            get { return _frame.Count; }
        }

        /// <summary>
        ///   Gets the <see cref="System.Object" /> at the specified index.
        /// </summary>
        /// <value> The <see cref="System.Object" /> . </value>
        /// <param name="index"> The index. </param>
        /// <returns> </returns>
        public object this[int index]
        {
            get
            {
                CqlColumn column = Schema[index];
                return column.Deserialize(CurrentValues[index]);
            }
        }

        /// <summary>
        ///   Gets the <see cref="System.Object" /> with the specified name.
        /// </summary>
        /// <value> The <see cref="System.Object" /> . </value>
        /// <param name="name"> The name. </param>
        /// <returns> </returns>
        public object this[string name]
        {
            get
            {
                CqlColumn column = Schema[name];
                return column.Deserialize(CurrentValues[column.Index]);
            }
        }

        #region ICqlQueryResult Members

        /// <summary>
        ///   Gets the type of the result.
        /// </summary>
        /// <value> return CqlResultType.Rows </value>
        public CqlResultType ResultType
        {
            get { return CqlResultType.Rows; }
        }

        /// <summary>
        ///   Gets the tracing id.
        /// </summary>
        /// <value> The tracing id. </value>
        public Guid? TracingId
        {
            get { return _frame.TracingId; }
        }

        #endregion

        #region IDisposable Members

        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        /// <summary>
        ///   Forwards the reader to the next row async.
        /// </summary>
        /// <returns> </returns>
        public virtual async Task<bool> ReadAsync()
        {
            if (_frame.Count > 0)
            {
                CurrentValues = await _frame.ReadNextDataRowAsync();
                return true;
            }

            return false;
        }

        /// <summary>
        ///   Forwards the reader to the next row.
        /// </summary>
        public bool Read()
        {
            return ReadAsync().Result;
        }

        /// <summary>
        ///   Closes this instance.
        /// </summary>
        public void Close()
        {
            Dispose();
        }

        /// <summary>
        ///   Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"> <c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources. </param>
        protected virtual void Dispose(bool disposing)
        {
            if (Interlocked.CompareExchange(ref _disposed, 1, 0) == 0)
            {
                if (disposing)
                {
                    _frame.Dispose();
                }
            }
        }

        /// <summary>
        ///   Finalizes an instance of the <see cref="CqlDataReader" /> class.
        /// </summary>
        ~CqlDataReader()
        {
            Dispose(false);
        }
    }

    /// <summary>
    ///   Provides access to a set of Cql data rows as returned from a query
    /// </summary>
    /// <typeparam name="T"> </typeparam>
    public class CqlDataReader<T> : CqlDataReader, IEnumerable<T> where T : class, new()
    {
        /// <summary>
        ///   The last read (and requested) value
        /// </summary>
        private T _current;

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlDataReader{T}" /> class.
        /// </summary>
        /// <param name="frame"> The frame. </param>
        internal CqlDataReader(ResultFrame frame)
            : base(frame)
        {
        }

        /// <summary>
        ///   Gets the last read value of this reader instance.
        /// </summary>
        /// <value> The current value. </value>
        public T Current
        {
            get
            {
                if (CurrentValues != null && _current == null)
                {
                    var value = new T();
                    ObjectAccessor accessor = ObjectAccessor.GetAccessor<T>();

                    foreach (CqlColumn column in Schema)
                    {
                        accessor.TrySetValue(column, value, this[column.Index]);
                    }

                    _current = value;
                }

                return _current;
            }
        }

        #region IEnumerable<T> Members

        /// <summary>
        ///   Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns> A <see cref="T:System.Collections.Generic.IEnumerator`1" /> that can be used to iterate through the collection. </returns>
        /// <filterpriority>1</filterpriority>
        public IEnumerator<T> GetEnumerator()
        {
            while (Read())
            {
                yield return Current;
            }
        }

        /// <summary>
        ///   Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns> An <see cref="T:System.Collections.IEnumerator" /> object that can be used to iterate through the collection. </returns>
        /// <filterpriority>2</filterpriority>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        /// <summary>
        ///   Forwards the reader to the next row async.
        /// </summary>
        /// <returns> </returns>
        public override Task<bool> ReadAsync()
        {
            _current = null;
            return base.ReadAsync();
        }
    }
}