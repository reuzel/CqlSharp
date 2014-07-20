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
using System.IO;
using System.Linq;
using CqlSharp.Annotations;
using CqlSharp.Protocol;
using CqlSharp.Serialization;

namespace CqlSharp.Network.Partition
{
    /// <summary>
    /// Key indicating the storage partition a row belongs to
    /// </summary>
    public class PartitionKey
    {
        /// <summary>
        /// An empty, unset partitionkey
        /// </summary>
        public readonly static PartitionKey None = new PartitionKey();

        private CqlType[] _keyTypes;
        private object[] _keyValues;
       
        /// <summary>
        /// Gets a value indicating whether a value is set for this PartitionKey
        /// </summary>
        /// <value> <c>true</c> if this instance is set; otherwise, <c>false</c> . </value>
        public bool IsSet
        {
            get { return _keyTypes == null; }
        }

        /// <summary>
        /// Sets the partition key to the provided value
        /// </summary>
        /// <param name="type"> The typeCode in which the value is represented in Cassandra. </param>
        /// <param name="value"> The value of the partition key column. </param>
        public void Set([NotNull] CqlType type, [NotNull] Object value)
        {
            if(type == null) throw new ArgumentNullException("type");
            if(value == null) throw new ArgumentNullException("value");

            _keyTypes = new[] {type};
            _keyValues = new[] {value};
        }

        /// <summary>
        /// Sets the partition key based on the provided values. Use this when composite partition keys are used
        /// </summary>
        /// <param name="types"> The typeCodes in which the values are represented in Cassandra. </param>
        /// <param name="values">
        /// The values of the partition key columns. The values must be given in the same order as the
        /// partition key is defined.
        /// </param>
        public void Set(CqlType[] types, Object[] values)
        {
            if(types == null)
                throw new ArgumentNullException("types");

            if(values == null)
                throw new ArgumentNullException("values");


            if(types.Length != values.Length)
                throw new ArgumentException("types and value collections are not of equal length");

            _keyTypes = types;
            _keyValues = values;

        }

        /// <summary>
        /// Sets the partitionkey based on the provided data object. Use CqlColumnAttribute to mark
        /// the relevant columns as PartitionKey column.
        /// </summary>
        /// <typeparam name="T"> </typeparam>
        /// <param name="data"> The data. </param>
        public void Set<T>(T data)
        {
            var accessor = ObjectAccessor<T>.Instance;
            accessor.SetPartitionKey(this, data);
        }

        internal byte[] Serialize()
        {
            var rawValues = new byte[_keyTypes.Length][];
            for (int i = 0; i < _keyTypes.Length; i++)
            {
                rawValues[i] = _keyTypes[i].Serialize(_keyValues[i], 1); //should work from protocol version 1 and upwards
            }

            int length = _keyTypes.Length * 3 + rawValues.Sum(val => val.Length);
            using (var stream = new MemoryStream(length))
            {
                foreach (var rawValue in rawValues)
                {
                    stream.WriteShortByteArray(rawValue);
                    stream.WriteByte(0);
                }

                return stream.ToArray();
            }
        }

        /// <summary>
        /// Clears this instance.
        /// </summary>
        public void Clear()
        {
            _keyTypes = null;
            _keyValues = null;
        }
    }
}