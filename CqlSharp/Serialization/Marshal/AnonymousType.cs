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
using System.Data;
using System.Text;

namespace CqlSharp.Serialization.Marshal
{
    /// <summary>
    /// Anonymous type, representing anonymous .Net objects that are used as input
    /// to Cql queries.
    /// </summary>
    public class AnonymousType<T> : CqlType<T>
    {
        /// <summary>
        /// Gets the CQL type code.
        /// </summary>
        /// <value>
        /// The CQL type code.
        /// </value>
        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Custom; }
        }

        /// <summary>
        /// Gets the name of the type.
        /// </summary>
        /// <param name="builder"></param>
        /// <returns>
        /// The name of the type.
        /// </returns>
        public override void AppendTypeName(StringBuilder builder)
        {
            throw new NotSupportedException("Anonymous types can not be part of any type name");
        }

        /// <summary>
        /// Gets the .Net type that represents this CqlType.
        /// </summary>
        /// <value>
        /// The type.
        /// </value>
        public override Type Type
        {
            get { return typeof(T); }
        }

        /// <summary>
        /// Gets the maximum size in bytes of values of this type.
        /// </summary>
        /// <value>
        /// The maximum size in bytes.
        /// </value>
        public override int Size
        {
            get { return 2000000000; }
        }

        /// <summary>
        /// gets the corresponding the DbType
        /// </summary>
        /// <returns></returns>
        public override DbType ToDbType()
        {
            return DbType.Object;
        }

        /// <summary>
        /// Serializes the specified value.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="protocolVersion"></param>
        /// <returns></returns>
        public override byte[] Serialize(T value, byte protocolVersion)
        {
            throw new NotSupportedException(
                "Anonymous types can not be serialized without additional type information. When inserting or updating, please prepare your query, or provide an explicit CqlType to the corresponding parameter.");
        }

        /// <summary>
        /// Deserializes the specified data.
        /// </summary>
        /// <param name="data">The data.</param>
        /// <param name="protocolVersion"></param>
        /// <returns></returns>
        public override T Deserialize(byte[] data, byte protocolVersion)
        {
            throw new NotSupportedException(
                "Anonymous types can not be deserialized without additional type information.");
        }
    }
}