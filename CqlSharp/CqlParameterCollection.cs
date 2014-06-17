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

using CqlSharp.Protocol;
using CqlSharp.Serialization;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;

namespace CqlSharp
{
    /// <summary>
    ///   A collection of CqlParameters to be used with CqlCommands
    /// </summary>
    public class CqlParameterCollection : DbParameterCollection
    {
        private readonly List<CqlParameter> _parameters;
        private readonly object _syncLock = new object();
        private MetaData _metaData;

        public CqlParameterCollection()
        {
            _parameters = new List<CqlParameter>();
        }

        internal CqlParameterCollection(MetaData metaData)
        {
            _metaData = metaData;
            _parameters = new List<CqlParameter>();
            foreach (Column column in metaData)
            {
                _parameters.Add(new CqlParameter(column));
            }
        }

        /// <summary>
        ///   Gets or sets the <see cref="CqlParameter" /> with the specified parameter name.
        /// </summary>
        /// <value> The <see cref="CqlParameter" /> . </value>
        /// <param name="paramName"> Name of the parameter. </param>
        /// <returns> </returns>
        public virtual new CqlParameter this[string paramName]
        {
            get { return GetCqlParameter(paramName); }
            set { SetParameter(paramName, value); }
        }

        /// <summary>
        ///   Gets and sets the <see cref="T:System.Data.Common.DbParameter" /> at the specified index.
        /// </summary>
        /// <param name="index"> The index. </param>
        /// <returns> </returns>
        public virtual new CqlParameter this[int index]
        {
            get { return _parameters[index]; }
            set { SetParameter(index, value); }
        }

        /// <summary>
        ///   Specifies the number of items in the collection.
        /// </summary>
        /// <returns> The number of items in the collection. </returns>
        /// <filterpriority>1</filterpriority>
        public override int Count
        {
            get { return _parameters.Count; }
        }

        /// <summary>
        ///   Specifies the <see cref="T:System.Object" /> to be used to synchronize access to the collection.
        /// </summary>
        /// <returns> A <see cref="T:System.Object" /> to be used to synchronize access to the <see
        ///    cref="T:System.Data.Common.DbParameterCollection" /> . </returns>
        /// <filterpriority>2</filterpriority>
        public override object SyncRoot
        {
            get { return _syncLock; }
        }

        /// <summary>
        ///   Specifies whether the collection is a fixed size.
        /// </summary>
        /// <returns> true if the collection is a fixed size; otherwise false. </returns>
        /// <filterpriority>1</filterpriority>
        public override bool IsFixedSize
        {
            get { return IsReadOnly; }
        }

        /// <summary>
        ///   Specifies whether the collection is read-only.
        /// </summary>
        /// <returns> true if the collection is read-only; otherwise false. </returns>
        /// <filterpriority>1</filterpriority>
        public override bool IsReadOnly
        {
            get { return _metaData != null; }
        }

        /// <summary>
        ///   Specifies whether the collection is synchronized.
        /// </summary>
        /// <returns> true if the collection is synchronized; otherwise false. </returns>
        /// <filterpriority>2</filterpriority>
        public override bool IsSynchronized
        {
            get { return false; }
        }

        /// <summary>
        ///   Gets the serialized values of this CqlParameterCollection
        /// </summary>
        /// <value> The values. </value>
        internal byte[][] Values
        {
            get
            {
                var values = new byte[Count][];
                for (int i = 0; i < Count; i++)
                {
                    CqlParameter param = this[i];

                    //skip if parameter has a null value
                    if (param.Value == DBNull.Value || param.Value==null) 
                        continue;

                    values[i] = param.CqlType.Serialize(param.Value);
                }

                return values;
            }
        }

        /// <summary>
        ///   Fixates this instance.
        /// </summary>
        internal void Fixate()
        {
            if (_metaData == null)
            {
                _metaData = new MetaData();
                foreach (CqlParameter param in _parameters)
                {
                    //add corresponding column to the metaData
                    _metaData.Add(param.Column);

                    //make it unchangable in typeCode and name
                    param.IsFixed = true;
                }
            }
        }

        /// <summary>
        ///   Adds the specified <see cref="T:System.Data.Common.DbParameter" /> object to the <see
        ///    cref="T:System.Data.Common.DbParameterCollection" />.
        /// </summary>
        /// <returns> The index of the <see cref="T:System.Data.Common.DbParameter" /> object in the collection. </returns>
        /// <param name="value"> The <see cref="P:System.Data.Common.DbParameter.Value" /> of the <see
        ///    cref="T:System.Data.Common.DbParameter" /> to add to the collection. </param>
        /// <filterpriority>1</filterpriority>
        public override int Add(object value)
        {
            return Add((CqlParameter)value);
        }

        /// <summary>
        ///   Adds the specified parameter.
        /// </summary>
        /// <param name="parameter"> The parameter. </param>
        /// <returns> </returns>
        public virtual int Add(CqlParameter parameter)
        {
            CheckIfFixed();

            _parameters.Add(parameter);
            return _parameters.Count - 1;
        }

        /// <summary>
        ///   Adds a new parameter with the specified name and value. The name will be
        ///   parsed to extract table and keyspace information (if any). The parameter typeCode
        ///   will be guessed from the object value.
        /// </summary>
        /// <param name="name"> The name. </param>
        /// <param name="value"> The value. </param>
        /// <returns> </returns>
        public virtual CqlParameter Add(string name, object value)
        {
            var parameter = new CqlParameter(name, value);
            Add(parameter);
            return parameter;
        }

        /// <summary>
        ///   Adds a new parameter with the specified name and typeCode
        /// </summary>
        /// <param name="name"> The name. </param>
        /// <param name="type"> The type of the parameter. </param>
        /// <returns> </returns>
        public virtual CqlParameter Add(string name, CqlType type)
        {
            var parameter = new CqlParameter(name, type);
            Add(parameter);
            return parameter;
        }

        /// <summary>
        ///   Adds a new parameter with the specified name and typeCode
        /// </summary>
        /// <param name="table"> The table. </param>
        /// <param name="name"> The name. </param>
        /// <param name="type"> The type </param>
        /// <returns> </returns>
        public virtual CqlParameter Add(string table, string name, CqlType type)
        {
            var parameter = new CqlParameter(table, name, type);
            Add(parameter);
            return parameter;
        }

        /// <summary>
        ///   Adds a new parameter with the specified name and typeCode
        /// </summary>
        /// <param name="keyspace"> The name of the keyspace. </param>
        /// <param name="table"> The name of the table. </param>
        /// <param name="name"> The name of the column. </param>
        /// <param name="type"> The type. </param>
        /// <returns> </returns>
        public virtual CqlParameter Add(string keyspace, string table, string name, CqlType type)
        {
            var parameter = new CqlParameter(keyspace, table, name, type);
            Add(parameter);
            return parameter;
        }

        /// <summary>
        ///   Checks the difference fixed.
        /// </summary>
        /// <exception cref="System.InvalidOperationException">Collection is readonly</exception>
        private void CheckIfFixed()
        {
            if (IsReadOnly)
                throw new InvalidOperationException(
                    "Can't change the CqlParameterCollection after it has been used to prepare or run a query");
        }

        /// <summary>
        ///   Indicates whether a <see cref="T:System.Data.Common.DbParameter" /> with the specified <see
        ///    cref="P:System.Data.Common.DbParameter.Value" /> is contained in the collection.
        /// </summary>
        /// <returns> true if the <see cref="T:System.Data.Common.DbParameter" /> is in the collection; otherwise false. </returns>
        /// <param name="value"> The <see cref="P:System.Data.Common.DbParameter.Value" /> of the <see
        ///    cref="T:System.Data.Common.DbParameter" /> to look for in the collection. </param>
        /// <filterpriority>1</filterpriority>
        public override bool Contains(object value)
        {
            return _parameters.Contains((CqlParameter)value);
        }

        /// <summary>
        ///   Removes all <see cref="T:System.Data.Common.DbParameter" /> values from the <see
        ///    cref="T:System.Data.Common.DbParameterCollection" />.
        /// </summary>
        /// <filterpriority>1</filterpriority>
        public override void Clear()
        {
            CheckIfFixed();

            _parameters.Clear();
        }

        /// <summary>
        ///   Returns the index of the specified <see cref="T:System.Data.Common.DbParameter" /> object.
        /// </summary>
        /// <returns> The index of the specified <see cref="T:System.Data.Common.DbParameter" /> object. </returns>
        /// <param name="value"> The <see cref="T:System.Data.Common.DbParameter" /> object in the collection. </param>
        /// <filterpriority>2</filterpriority>
        public override int IndexOf(object value)
        {
            return _parameters.IndexOf((CqlParameter)value);
        }

        /// <summary>
        ///   Inserts the specified index of the <see cref="T:System.Data.Common.DbParameter" /> object with the specified name into the collection at the specified index.
        /// </summary>
        /// <param name="index"> The index at which to insert the <see cref="T:System.Data.Common.DbParameter" /> object. </param>
        /// <param name="value"> The <see cref="T:System.Data.Common.DbParameter" /> object to insert into the collection. </param>
        /// <filterpriority>1</filterpriority>
        public override void Insert(int index, object value)
        {
            CheckIfFixed();

            var param = (CqlParameter)value;
            _parameters.Insert(index, param);
        }

        /// <summary>
        ///   Removes the specified <see cref="T:System.Data.Common.DbParameter" /> object from the collection.
        /// </summary>
        /// <param name="value"> The <see cref="T:System.Data.Common.DbParameter" /> object to remove. </param>
        /// <filterpriority>1</filterpriority>
        public override void Remove(object value)
        {
            CheckIfFixed();

            var param = (CqlParameter)value;
            _parameters.Remove(param);
        }

        /// <summary>
        ///   Removes the <see cref="T:System.Data.Common.DbParameter" /> object at the specified from the collection.
        /// </summary>
        /// <param name="index"> The index where the <see cref="T:System.Data.Common.DbParameter" /> object is located. </param>
        /// <filterpriority>2</filterpriority>
        public override void RemoveAt(int index)
        {
            CheckIfFixed();
            _parameters.RemoveAt(index);
        }

        /// <summary>
        ///   Removes the <see cref="T:System.Data.Common.DbParameter" /> object with the specified name from the collection.
        /// </summary>
        /// <param name="parameterName"> The name of the <see cref="T:System.Data.Common.DbParameter" /> object to remove. </param>
        /// <filterpriority>2</filterpriority>
        public override void RemoveAt(string parameterName)
        {
            CheckIfFixed();

            int index = GetIndex(parameterName);
            _parameters.RemoveAt(index);
        }

        /// <summary>
        ///   Sets the <see cref="T:System.Data.Common.DbParameter" /> object at the specified index to a new value.
        /// </summary>
        /// <param name="index"> The index where the <see cref="T:System.Data.Common.DbParameter" /> object is located. </param>
        /// <param name="value"> The new <see cref="T:System.Data.Common.DbParameter" /> value. </param>
        protected override void SetParameter(int index, DbParameter value)
        {
            SetParameter(index, (CqlParameter)value);
        }

        /// <summary>
        ///   Sets the <see cref="T:CqlSharp.CqlParameter" /> object with the specified name to a new value.
        /// </summary>
        private void SetParameter(int index, CqlParameter value)
        {
            CheckIfFixed();
            _parameters[index] = value;
        }

        /// <summary>
        ///   Sets the <see cref="T:System.Data.Common.DbParameter" /> object with the specified name to a new value.
        /// </summary>
        /// <param name="parameterName"> The name of the <see cref="T:System.Data.Common.DbParameter" /> object in the collection. </param>
        /// <param name="value"> The new <see cref="T:System.Data.Common.DbParameter" /> value. </param>
        protected override void SetParameter(string parameterName, DbParameter value)
        {
            SetParameter(parameterName, (CqlParameter)value);
        }

        /// <summary>
        ///   Sets the <see cref="T:CqlSharp.CqlParameter" /> object with the specified name to a new value.
        /// </summary>
        /// <param name="parameterName"> The name of the <see cref="T:System.Data.Common.DbParameter" /> object in the collection. </param>
        /// <param name="value"> The new <see cref="T:System.Data.Common.DbParameter" /> value. </param>
        private void SetParameter(string parameterName, CqlParameter value)
        {
            CheckIfFixed();
            int index = GetIndex(parameterName);
            _parameters[index] = value;
        }

        /// <summary>
        ///   Returns the index of the <see cref="T:System.Data.Common.DbParameter" /> object with the specified name.
        /// </summary>
        /// <returns> The index of the <see cref="T:System.Data.Common.DbParameter" /> object with the specified name. </returns>
        /// <param name="parameterName"> The name of the <see cref="T:System.Data.Common.DbParameter" /> object in the collection. </param>
        /// <filterpriority>2</filterpriority>
        public override int IndexOf(string parameterName)
        {
            if (parameterName == null)
                throw new ArgumentNullException("parameterName");

            if (_metaData != null)
            {
                Column c;
                if (_metaData.TryGetValue(parameterName, out c))
                    return c.Index;

                return -1;
            }

            return _parameters.FindIndex(
                param =>
                param.Column.KeySpaceTableAndName.Equals(parameterName) ||
                param.Column.TableAndName.Equals(parameterName) ||
                param.Column.Name.Equals(parameterName));
        }

        /// <summary>
        ///   Gets the index of the parameter with the given name.
        /// </summary>
        /// <param name="parameterName"> Name of the parameter. </param>
        /// <returns> </returns>
        /// <exception cref="System.IndexOutOfRangeException">Parameter with the given name is not found</exception>
        private int GetIndex(string parameterName)
        {
            int index = IndexOf(parameterName);
            if (index < 0)
                throw new IndexOutOfRangeException("Parameter with the given name is not found");

            return index;
        }

        /// <summary>
        ///   Exposes the <see cref="M:System.Collections.IEnumerable.GetEnumerator" /> method, which supports a simple iteration over a collection by a .NET Framework data provider.
        /// </summary>
        /// <returns> An <see cref="T:System.Collections.IEnumerator" /> that can be used to iterate through the collection. </returns>
        /// <filterpriority>2</filterpriority>
        public override IEnumerator GetEnumerator()
        {
            return _parameters.GetEnumerator();
        }

        /// <summary>
        ///   Returns the <see cref="T:System.Data.Common.DbParameter" /> object at the specified index in the collection.
        /// </summary>
        /// <returns> The <see cref="T:System.Data.Common.DbParameter" /> object at the specified index in the collection. </returns>
        /// <param name="index"> The index of the <see cref="T:System.Data.Common.DbParameter" /> in the collection. </param>
        protected override DbParameter GetParameter(int index)
        {
            return _parameters[index];
        }

        /// <summary>
        ///   Returns <see cref="T:System.Data.Common.DbParameter" /> the object with the specified name.
        /// </summary>
        /// <returns> The <see cref="T:System.Data.Common.DbParameter" /> the object with the specified name. </returns>
        /// <param name="parameterName"> The name of the <see cref="T:System.Data.Common.DbParameter" /> in the collection. </param>
        protected override DbParameter GetParameter(string parameterName)
        {
            return GetCqlParameter(parameterName);
        }

        /// <summary>
        ///   Gets the CQL parameter.
        /// </summary>
        /// <param name="parameterName"> Name of the parameter. </param>
        /// <returns> </returns>
        private CqlParameter GetCqlParameter(string parameterName)
        {
            int index = GetIndex(parameterName);
            return _parameters[index];
        }

        /// <summary>
        ///   Indicates whether a <see cref="T:System.Data.Common.DbParameter" /> with the specified name exists in the collection.
        /// </summary>
        /// <returns> true if the <see cref="T:System.Data.Common.DbParameter" /> is in the collection; otherwise false. </returns>
        /// <param name="value"> The name of the <see cref="T:System.Data.Common.DbParameter" /> to look for in the collection. </param>
        /// <filterpriority>1</filterpriority>
        public override bool Contains(string value)
        {
            return IndexOf(value) > 0;
        }

        /// <summary>
        ///   Copies an array of items to the collection starting at the specified index.
        /// </summary>
        /// <param name="array"> The array of items to copy to the collection. </param>
        /// <param name="index"> The index in the collection to copy the items. </param>
        /// <filterpriority>2</filterpriority>
        public override void CopyTo(Array array, int index)
        {
            if (array == null)
                throw new ArgumentNullException("array");

            var c = (ICollection)_parameters;
            c.CopyTo(array, index);
        }

        /// <summary>
        ///   Adds an array of items with the specified values to the <see cref="T:System.Data.Common.DbParameterCollection" />.
        /// </summary>
        /// <param name="values"> An array of values of typeCode <see cref="T:System.Data.Common.DbParameter" /> to add to the collection. </param>
        /// <filterpriority>2</filterpriority>
        public override void AddRange(Array values)
        {
            if (values == null)
                throw new ArgumentNullException("values");

            foreach (object obj in values)
            {
                if (!(obj is CqlParameter))
                    throw new ArgumentException("All values must be CqlParameter instances");
            }

            foreach (CqlParameter cqlParameter in values)
            {
                _parameters.Add(cqlParameter);
            }
        }

        /// <summary>
        ///   Sets the parameters to the values as defined by the properties of the provided object.
        /// </summary>
        /// <typeparam name="T"> Type of the object holding the parameter values. The names of the properties must match the names of the columns of the ResultMetaData of the prepared query for them to be usable. </typeparam>
        /// <param name="source"> The object holding the parameter values. </param>
        public virtual void Set<T>(T source)
        {
            ObjectAccessor<T> accessor = ObjectAccessor<T>.Instance;
            foreach (var parameter in _parameters)
            {
                string name;
                if (accessor.IsKeySpaceSet)
                    name = parameter.Column.KeySpaceTableAndName;
                else if (accessor.IsNameSet)
                    name = parameter.Column.TableAndName;
                else
                    name = parameter.Column.Name;

                object value;
                if (accessor.TryGetValue(name, source, out value))
                {
                    parameter.Value = value;
                }
            }
        }
    }
}