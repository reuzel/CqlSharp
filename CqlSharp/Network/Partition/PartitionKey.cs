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
using System.IO;
using System.Linq;
using CqlSharp.Protocol;
using CqlSharp.Serialization;

namespace CqlSharp.Network.Partition
{
    /// <summary>
    ///   Key indicating the storage partition a row belongs to
    /// </summary>
    public class PartitionKey
    {
        /// <summary>
        ///   The none
        /// </summary>
        public static readonly PartitionKey None = default(PartitionKey);

        /// <summary>
        ///   The actual partition key value
        /// </summary>
        private byte[] _key;

        /// <summary>
        ///   Gets the partition key value
        /// </summary>
        /// <value> The key. </value>
        internal byte[] Key
        {
            get { return _key == null ? null : (byte[]) _key.Clone(); }
        }

        /// <summary>
        ///   Gets a value indicating whether a value is set for this PartitionKey
        /// </summary>
        /// <value> <c>true</c> if this instance is set; otherwise, <c>false</c> . </value>
        public bool IsSet
        {
            get { return _key != null; }
        }

        /// <summary>
        ///   Copies this instance.
        /// </summary>
        /// <returns> </returns>
        internal PartitionKey Copy()
        {
            byte[] key = Key;
            return new PartitionKey {_key = key};
        }

        /// <summary>
        ///   Sets the partition key to the provided value
        /// </summary>
        /// <param name="type"> The type in which the value is represented in Cassandra. </param>
        /// <param name="value"> The value of the partition key column. </param>
        public void Set(CqlType type, Object value)
        {
            if (value == null)
                throw new ArgumentNullException("value");

            _key = ValueSerialization.Serialize(type, value);
        }

        /// <summary>
        ///   Sets the partition key based on the provided values. Use this when composite partition keys are used
        /// </summary>
        /// <param name="types"> The types in which the values are represented in Cassandra. </param>
        /// <param name="values"> The values of the partition key columns. The values must be given in the same order as the partition key is defined. </param>
        public void Set(CqlType[] types, Object[] values)
        {
            if (types == null)
                throw new ArgumentNullException("types");

            if (values == null)
                throw new ArgumentNullException("values");


            if (types.Length != values.Length)
                throw new ArgumentException("types and values are not of equal length");

            var rawValues = new byte[types.Length][];
            for (int i = 0; i < types.Length; i++)
            {
                rawValues[i] = ValueSerialization.Serialize(types[i], values[i]);
            }

            int length = types.Length*3 + rawValues.Sum(val => val.Length);
            using (var stream = new MemoryStream(length))
            {
                foreach (var rawValue in rawValues)
                {
                    stream.WriteShortByteArray(rawValue);
                    stream.WriteByte(0);
                }

                _key = stream.ToArray();
            }
        }

        /// <summary>
        ///   Sets the partitionkey based on the provided data object. Use CqlColumnAttribute to mark
        ///   the relevant columns as PartitionKey column.
        /// </summary>
        /// <typeparam name="T"> </typeparam>
        /// <param name="data"> The data. </param>
        public void Set<T>(T data)
        {
            var accessor = ObjectAccessor<T>.Instance;
            accessor.SetPartitionKey(this, data);
        }

        /// <summary>
        ///   Clears this instance.
        /// </summary>
        public void Clear()
        {
            _key = null;
        }

        public bool Equals(PartitionKey other)
        {
            return _key.SequenceEqual(other._key);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is PartitionKey && Equals((PartitionKey) obj);
        }

        public override int GetHashCode()
        {
            return (_key != null ? _key.GetHashCode() : 0);
        }
    }
}