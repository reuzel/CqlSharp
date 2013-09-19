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
using System.Data;
using System.Data.Common;
using System.Net;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using CqlSharp.Protocol;
using CqlSharp.Serialization;

namespace CqlSharp
{
    /// <summary>
    ///   Provides access to a set of Cql data rows as returned from a query
    /// </summary>
    public class CqlDataReader : DbDataReader, ICqlQueryResult
    {
        private readonly CqlConnection _connectionToClose;
        private readonly ResultFrame _frame;
        protected byte[][] CurrentValues;
        private int _disposed;

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlDataReader" /> class.
        /// </summary>
        /// <param name="frame"> The frame. </param>
        /// <param name="connectionToClose"> connection to close when done </param>
        internal CqlDataReader(ResultFrame frame, CqlConnection connectionToClose)
        {
            _frame = frame;
            _connectionToClose = connectionToClose;
        }

        /// <summary>
        ///   Gets the ResultMetaData.
        /// </summary>
        /// <value> The ResultMetaData. </value>
        internal MetaData MetaData
        {
            get
            {
                if (_frame.ResultMetaData == null)
                    throw new InvalidOperationException("No column metadata has been retrieved.");

                return _frame.ResultMetaData;
            }
        }

        /// <summary>
        ///   Gets a value indicating whether this reader instance has more rows.
        /// </summary>
        /// <value> <c>true</c> if this instance has more rows; otherwise, <c>false</c> . </value>
        public override bool HasRows
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
        public override object this[int index]
        {
            get
            {
                Column column = MetaData[index];
                object value = ValueSerialization.Deserialize(column.CqlType, column.CollectionKeyType,
                                                              column.CollectionValueType, CurrentValues[index]);
                return value ?? DBNull.Value;
            }
        }

        /// <summary>
        ///   Gets the <see cref="System.Object" /> with the specified name.
        /// </summary>
        /// <value> The <see cref="System.Object" /> . </value>
        /// <param name="name"> The name. </param>
        /// <returns> </returns>
        public override object this[string name]
        {
            get
            {
                Column column = MetaData[name];
                object value = ValueSerialization.Deserialize(column.CqlType, column.CollectionKeyType,
                                                              column.CollectionValueType, CurrentValues[column.Index]);
                return value ?? DBNull.Value;
            }
        }

        /// <summary>
        ///   Gets a value indicating the depth of nesting for the current row.
        /// </summary>
        /// <returns> The level of nesting. </returns>
        public override int Depth
        {
            get { return 0; }
        }

        /// <summary>
        ///   Gets a value indicating whether the data reader is closed.
        /// </summary>
        /// <returns> true if the data reader is closed; otherwise, false. </returns>
        public override bool IsClosed
        {
            get { return _disposed == 1; }
        }

        /// <summary>
        ///   Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.
        /// </summary>
        /// <returns> The number of rows changed, inserted, or deleted; 0 if no rows were affected or the statement failed; and -1 for SELECT statements. </returns>
        /// <exception cref="System.NotSupportedException">Cql does not provide information on records affected</exception>
        public override int RecordsAffected
        {
            get
            {
                //CqlDataReader is only used with select statements
                return -1;
            }
        }

        /// <summary>
        ///   Gets the number of columns in the current row.
        /// </summary>
        /// <returns> When not positioned in a valid recordset, 0; otherwise, the number of columns in the current record. The default is -1. </returns>
        public override int FieldCount
        {
            get
            {
                if (CurrentValues == null)
                    return 0;

                return CurrentValues.Length;
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

        /// <summary>
        ///   Forwards the reader to the next row async.
        /// </summary>
        /// <returns> </returns>
        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            if (_frame.Count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                CurrentValues = await _frame.ReadNextDataRowAsync().ConfigureAwait(false);
                return true;
            }

            return false;
        }

        /// <summary>
        ///   Forwards the reader to the next row.
        /// </summary>
        /// <returns> true if there are more rows; otherwise, false. </returns>
        public override bool Read()
        {
            return ReadAsync().Result;
        }

        /// <summary>
        ///   Closes this instance.
        /// </summary>
        public override void Close()
        {
            Dispose();
        }

        /// <summary>
        ///   Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"> <c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources. </param>
        protected override void Dispose(bool disposing)
        {
            if (Interlocked.CompareExchange(ref _disposed, 1, 0) == 0)
            {
                if (disposing)
                {
                    if (_connectionToClose != null)
                        _connectionToClose.Close();

                    _frame.Dispose();
                }
            }
        }

        /// <summary>
        ///   Returns a <see cref="T:System.Data.DataTable" /> that describes the column metadata of the <see
        ///    cref="T:System.Data.IDataReader" />.
        /// </summary>
        /// <returns> A <see cref="T:System.Data.DataTable" /> that describes the column metadata. </returns>
        public override DataTable GetSchemaTable()
        {
            var table = new DataTable();
            table.Columns.Add(CqlSchemaTableColumnNames.ColumnOrdinal, typeof (int));
            table.Columns.Add(CqlSchemaTableColumnNames.KeySpaceName, typeof (string));
            table.Columns.Add(CqlSchemaTableColumnNames.TableName, typeof (string));
            table.Columns.Add(CqlSchemaTableColumnNames.ColumnName, typeof (string));
            table.Columns.Add(CqlSchemaTableColumnNames.CqlType, typeof (string));
            table.Columns.Add(CqlSchemaTableColumnNames.CollectionKeyType, typeof (string));
            table.Columns.Add(CqlSchemaTableColumnNames.CollectionValueType, typeof (string));
            table.Columns.Add(CqlSchemaTableColumnNames.Type, typeof (string));

            foreach (var column in MetaData)
            {
                var row = table.NewRow();
                row[CqlSchemaTableColumnNames.ColumnOrdinal] = column.Index;
                row[CqlSchemaTableColumnNames.KeySpaceName] = column.Keyspace;
                row[CqlSchemaTableColumnNames.TableName] = column.Table;
                row[CqlSchemaTableColumnNames.ColumnName] = column.Name;
                row[CqlSchemaTableColumnNames.CqlType] = column.CqlType.ToString();
                row[CqlSchemaTableColumnNames.CollectionKeyType] = column.CollectionKeyType.HasValue
                                                                       ? column.CollectionKeyType.ToString()
                                                                       : null;
                row[CqlSchemaTableColumnNames.CollectionValueType] = column.CollectionValueType.HasValue
                                                                         ? column.CollectionValueType.ToString()
                                                                         : null;
                row[CqlSchemaTableColumnNames.Type] = column.ToType().FullName;
                table.Rows.Add(row);
            }

            return table;
        }

        /// <summary>
        ///   Advances the data reader to the next result, when reading the results of batch SQL statements.
        /// </summary>
        /// <returns> true if there are more rows; otherwise, false. </returns>
        /// <exception cref="System.NotSupportedException">Cql does not support batched select statements</exception>
        public override bool NextResult()
        {
            //there are never any next resultsets
            return false;
        }

        /// <summary>
        ///   Gets the value of the specified column as a Boolean.
        /// </summary>
        /// <param name="i"> The zero-based column ordinal. </param>
        /// <returns> The value of the column. </returns>
        public override bool GetBoolean(int i)
        {
            if (CurrentValues[i] == null) return default(bool);
            return (bool) ValueSerialization.Deserialize(CqlType.Boolean, CurrentValues[i]);
        }

        /// <summary>
        ///   Gets the 8-bit unsigned integer value of the specified column.
        /// </summary>
        /// <param name="i"> The zero-based column ordinal. </param>
        /// <returns> The 8-bit unsigned integer value of the specified column. </returns>
        /// <exception cref="System.NotSupportedException">Single byte values are not supported by Cql</exception>
        public override byte GetByte(int i)
        {
            throw new NotSupportedException("Single byte values are not supported by Cql");
        }

        /// <summary>
        ///   Reads a stream of bytes from the specified column offset into the buffer as an array, starting at the given buffer offset.
        /// </summary>
        /// <param name="i"> The zero-based column ordinal. </param>
        /// <param name="fieldOffset"> The index within the field from which to start the read operation. </param>
        /// <param name="buffer"> The buffer into which to read the stream of bytes. </param>
        /// <param name="bufferoffset"> The index for <paramref name="buffer" /> to start the read operation. </param>
        /// <param name="length"> The number of bytes to read. </param>
        /// <returns> The actual number of bytes read. </returns>
        /// <exception cref="System.ArgumentException">Provided length allows more data to be copied than what fits in the provided buffer;length</exception>
        /// <exception cref="System.ArgumentOutOfRangeException">fieldOffset;The field offset is larger than the stored string</exception>
        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            if (bufferoffset + length > buffer.Length)
                throw new ArgumentException(
                    "Provided length allows more data to be copied than what fits in the provided buffer", "length");

            if (CurrentValues[i] == null)
                return 0;

            var value = (byte[]) ValueSerialization.Deserialize(CqlType.Blob, CurrentValues[i]);

            if (fieldOffset < 0 || fieldOffset >= value.Length)
                throw new ArgumentOutOfRangeException("fieldOffset", fieldOffset,
                                                      "The field offset is larger than the stored string");

            //copy string to buffer
            var copySize = (int) Math.Min(length, value.Length - fieldOffset);
            Buffer.BlockCopy(value, (int) fieldOffset, buffer, bufferoffset, copySize);
            return copySize;
        }

        /// <summary>
        ///   Gets the bytes (blob) data value of the specified field.
        /// </summary>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The bytes value of the specified field. </returns>
        public byte[] GetBytes(int i)
        {
            return (byte[]) ValueSerialization.Deserialize(CqlType.Blob, CurrentValues[i]);
        }

        /// <summary>
        ///   Gets the character value of the specified column.
        /// </summary>
        /// <param name="i"> The zero-based column ordinal. </param>
        /// <returns> The character value of the specified column. </returns>
        /// <exception cref="System.NotSupportedException">Single char values are not supported by Cql</exception>
        public override char GetChar(int i)
        {
            throw new NotSupportedException("Single char values are not supported by Cql");
        }

        /// <summary>
        ///   Reads a stream of characters from the specified column offset into the buffer as an array, starting at the given buffer offset.
        /// </summary>
        /// <param name="i"> The zero-based column ordinal. </param>
        /// <param name="fieldoffset"> The index within the row from which to start the read operation. </param>
        /// <param name="buffer"> The buffer into which to read the stream of bytes. </param>
        /// <param name="bufferoffset"> The index for <paramref name="buffer" /> to start the read operation. </param>
        /// <param name="length"> The number of bytes to read. </param>
        /// <returns> The actual number of characters read. </returns>
        /// <exception cref="System.ArgumentException">Provided length allows more data to be copied than what fits in the provided buffer;length</exception>
        /// <exception cref="System.ArgumentOutOfRangeException">fieldoffset;The field offset is larger than the stored string</exception>
        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            if (bufferoffset + length > buffer.Length)
                throw new ArgumentException(
                    "Provided length allows more data to be copied than what fits in the provided buffer", "length");

            if (CurrentValues[i] == null)
                return 0;

            var value = (string) ValueSerialization.Deserialize(CqlType.Varchar, CurrentValues[i]);

            if (fieldoffset < 0 || fieldoffset >= value.Length)
                throw new ArgumentOutOfRangeException("fieldoffset", fieldoffset,
                                                      "The field offset is larger than the stored string");

            //copy string to buffer
            var copySize = (int) Math.Min(length, value.Length - fieldoffset);
            value.CopyTo((int) fieldoffset, buffer, bufferoffset, copySize);
            return copySize;
        }

        /// <summary>
        ///   Gets the data type information for the specified field.
        /// </summary>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The data type information for the specified field. </returns>
        public override string GetDataTypeName(int i)
        {
            return MetaData[i].CqlType.ToString();
        }

        /// <summary>
        ///   Gets the date and time data value of the specified field.
        /// </summary>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The date and time data value of the specified field. </returns>
        public override DateTime GetDateTime(int i)
        {
            if (CurrentValues[i] == null) return default(DateTime);

            return (DateTime) ValueSerialization.Deserialize(CqlType.Timestamp, CurrentValues[i]);
        }

        /// <summary>
        ///   Gets the fixed-position numeric value of the specified field.
        /// </summary>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The fixed-position numeric value of the specified field. </returns>
        /// <exception cref="System.NotSupportedException"></exception>
        public override decimal GetDecimal(int i)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        ///   Gets the double-precision floating point number of the specified field.
        /// </summary>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The double-precision floating point number of the specified field. </returns>
        public override double GetDouble(int i)
        {
            if (CurrentValues[i] == null) return default(double);

            return (double) ValueSerialization.Deserialize(CqlType.Double, CurrentValues[i]);
        }

        /// <summary>
        ///   Gets the <see cref="T:System.Type" /> information corresponding to the type of <see cref="T:System.Object" /> that would be returned from <see
        ///    cref="M:System.Data.IDataRecord.GetValue(System.Int32)" />.
        /// </summary>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The <see cref="T:System.Type" /> information corresponding to the type of <see cref="T:System.Object" /> that would be returned from <see
        ///    cref="M:System.Data.IDataRecord.GetValue(System.Int32)" /> . </returns>
        public override Type GetFieldType(int i)
        {
            return MetaData[i].ToType();
        }

        /// <summary>
        ///   Gets the single-precision floating point number of the specified field.
        /// </summary>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The single-precision floating point number of the specified field. </returns>
        public override float GetFloat(int i)
        {
            if (CurrentValues[i] == null) return default(float);

            return (float) ValueSerialization.Deserialize(CqlType.Float, CurrentValues[i]);
        }

        /// <summary>
        ///   Returns the GUID value of the specified field.
        /// </summary>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The GUID value of the specified field. </returns>
        public override Guid GetGuid(int i)
        {
            if (CurrentValues[i] == null) return default(Guid);

            return (Guid) ValueSerialization.Deserialize(CqlType.Uuid, CurrentValues[i]);
        }

        /// <summary>
        ///   Gets the 16-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The 16-bit signed integer value of the specified field. </returns>
        /// <exception cref="System.NotSupportedException">short values are not supported by Cql</exception>
        public override short GetInt16(int i)
        {
            throw new NotSupportedException("short values are not supported by Cql");
        }

        /// <summary>
        ///   Gets the 32-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The 32-bit signed integer value of the specified field. </returns>
        public override int GetInt32(int i)
        {
            if (CurrentValues[i] == null) return default(int);
            return (int) ValueSerialization.Deserialize(CqlType.Int, CurrentValues[i]);
        }

        /// <summary>
        ///   Gets the 64-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The 64-bit signed integer value of the specified field. </returns>
        public override long GetInt64(int i)
        {
            if (CurrentValues[i] == null) return default(long);

            return (long) ValueSerialization.Deserialize(CqlType.Bigint, CurrentValues[i]);
        }

        /// <summary>
        ///   Gets the name for the field to find.
        /// </summary>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The name of the field or the empty string (""), if there is no value to return. </returns>
        public override string GetName(int i)
        {
            return MetaData[i].Name;
        }

        /// <summary>
        ///   Return the index of the named field.
        /// </summary>
        /// <param name="name"> The name of the field to find. </param>
        /// <returns> The index of the named field. </returns>
        public override int GetOrdinal(string name)
        {
            return MetaData[name].Index;
        }

        /// <summary>
        ///   Gets the string value of the specified field.
        /// </summary>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The string value of the specified field. </returns>
        public override string GetString(int i)
        {
            return (string) ValueSerialization.Deserialize(CqlType.Varchar, CurrentValues[i]);
        }

        /// <summary>
        ///   Gets the IPAddress value of the specified field.
        /// </summary>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The IPAddress value of the specified field. </returns>
        public IPAddress GetIPAddress(int i)
        {
            return (IPAddress) ValueSerialization.Deserialize(CqlType.Inet, CurrentValues[i]);
        }

        /// <summary>
        ///   Gets the BigInteger value of the specified field.
        /// </summary>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The BigInteger value of the specified field. </returns>
        public BigInteger GetBigInteger(int i)
        {
            if (CurrentValues[i] == null) return default(BigInteger);

            return (BigInteger) ValueSerialization.Deserialize(CqlType.Varint, CurrentValues[i]);
        }

        /// <summary>
        ///   Gets the Set value of the specified field.
        /// </summary>
        /// <typeparam name="T"> The type of the contents of the set </typeparam>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The Set value of the specified field. </returns>
        public HashSet<T> GetSet<T>(int i)
        {
            CqlType setType = typeof (T).ToCqlType();
            return (HashSet<T>) ValueSerialization.Deserialize(CqlType.Set, null, setType, CurrentValues[i]);
        }

        /// <summary>
        ///   Gets the List value of the specified field.
        /// </summary>
        /// <typeparam name="T"> The type of the contents of the list </typeparam>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The list value of the specified field. </returns>
        public List<T> GetList<T>(int i)
        {
            CqlType listType = typeof (T).ToCqlType();
            return (List<T>) ValueSerialization.Deserialize(CqlType.List, null, listType, CurrentValues[i]);
        }

        /// <summary>
        ///   Gets the Dictionary value of the specified field.
        /// </summary>
        /// <typeparam name="TKey"> The type of the key. </typeparam>
        /// <typeparam name="TValue"> The type of the value. </typeparam>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The list value of the specified field. </returns>
        public Dictionary<TKey, TValue> GetDictionary<TKey, TValue>(int i)
        {
            CqlType keyType = typeof (TKey).ToCqlType();
            CqlType valueType = typeof (TValue).ToCqlType();
            return
                (Dictionary<TKey, TValue>)
                ValueSerialization.Deserialize(CqlType.Map, keyType, valueType, CurrentValues[i]);
        }

        /// <summary>
        ///   Return the value of the specified field.
        /// </summary>
        /// <param name="i"> The index of the field to find. </param>
        /// <returns> The <see cref="T:System.Object" /> which will contain the field value upon return. </returns>
        public override object GetValue(int i)
        {
            return this[i];
        }

        /// <summary>
        ///   Populates an array of objects with the column values of the current record.
        /// </summary>
        /// <param name="values"> An array of <see cref="T:System.Object" /> to copy the attribute fields into. </param>
        /// <returns> The number of instances of <see cref="T:System.Object" /> in the array. </returns>
        /// <exception cref="System.ArgumentException">values array too small to fit row contents;values</exception>
        public override int GetValues(object[] values)
        {
            if (CurrentValues.Length > values.Length)
                throw new ArgumentException("values array too small to fit row contents", "values");

            for (int i = 0; i < CurrentValues.Length; i++)
            {
                values[i] = this[i];
            }

            return CurrentValues.Length;
        }

        public override bool IsDBNull(int i)
        {
            return CurrentValues[i] == null;
        }

        public override IEnumerator GetEnumerator()
        {
            return new DbEnumerator(this, false);
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
        /// <param name="connectionToClose"> connection to close when done </param>
        internal CqlDataReader(ResultFrame frame, CqlConnection connectionToClose)
            : base(frame, connectionToClose)
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
                    ObjectAccessor<T> accessor = ObjectAccessor<T>.Instance;

                    foreach (Column column in MetaData)
                    {
                        string name;
                        if (accessor.IsKeySpaceSet)
                        {
                            name = column.KeySpaceTableAndName;
                        }
                        else if (accessor.IsTableSet)
                        {
                            name = column.TableAndName;
                        }
                        else
                        {
                            name = column.Name;
                        }

                        var fieldValue = IsDBNull(column.Index) ? null : this[column.Index];
                        accessor.TrySetValue(name, value, fieldValue);
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
        public new IEnumerator<T> GetEnumerator()
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
        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            _current = null;
            return base.ReadAsync(cancellationToken);
        }
    }
}