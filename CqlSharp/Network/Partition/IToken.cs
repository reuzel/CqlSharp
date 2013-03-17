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
using System;
using System.IO;
using System.Linq;

namespace CqlSharp.Network.Partition
{
    public struct PartitionKey
    {
        private byte[] _key;

        /// <summary>
        /// Gets the partition key value
        /// </summary>
        /// <value>
        /// The key.
        /// </value>
        internal byte[] Key
        {
            get { return _key; }
        }

        /// <summary>
        /// Sets the partition key to the provided value
        /// </summary>
        /// <param name="type">The type in which the value is represented in Cassandra.</param>
        /// <param name="value">The value of the partition key column.</param>
        public void Set(CqlType type, Object value)
        {
            if (value == null)
                throw new ArgumentNullException("value");

            _key = ValueSerialization.Serialize(type, value);
        }

        /// <summary>
        /// Sets the partition key based on the provided values. Use this when composite partition keys are used
        /// </summary>
        /// <param name="types">The types in which the values are represented in Cassandra.</param>
        /// <param name="values">The values of the partition key columns. The values must be given in the same order as the partition key is defined.</param>
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

            int length = types.Length * 3 + rawValues.Sum(val => val.Length);
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

        public void Set(Object data)
        {
            //set values based on attributes
        }
    }

    /// <summary>
    /// Token as used to route a query to a node based on partition key column values.
    /// </summary>
    interface IToken : IComparable
    {
        /// <summary>
        ///   Parses the specified token STR.
        /// </summary>
        /// <param name="tokenStr"> The token STR. </param>
        void Parse(string tokenStr);

        /// <summary>
        ///   Parses the specified partition key.
        /// </summary>
        /// <param name="partitionKey"> The partition key. </param>
        void Parse(byte[] partitionKey);
    }
}