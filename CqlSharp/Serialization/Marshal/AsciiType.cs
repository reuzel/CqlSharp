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
    /// Ascii string type
    /// </summary>
    public class AsciiType : CqlType<string>
    {
        /// <summary>
        /// The singleton instance
        /// </summary>
        public static readonly AsciiType Instance = new AsciiType();

        /// <summary>
        /// type of string
        /// </summary>
        private static readonly Type AType = typeof(string);

        /// <summary>
        /// Gets the CQL type code.
        /// </summary>
        /// <value>
        /// The CQL type code.
        /// </value>
        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Ascii; }
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
            builder.Append("org.apache.cassandra.db.marshal.AsciiType");
        }

        /// <summary>
        /// Gets the .Net type that represents this CqlType.
        /// </summary>
        /// <value>
        /// The type.
        /// </value>
        public override Type Type
        {
            get { return AType; }
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
            return DbType.AnsiString;
        }

        /// <summary>
        /// Serializes the specified value.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="protocolVersion"></param>
        /// <returns></returns>
        public override byte[] Serialize(string value, byte protocolVersion)
        {
            return Encoding.ASCII.GetBytes(value);
        }

        /// <summary>
        /// Deserializes the specified data.
        /// </summary>
        /// <param name="data">The data.</param>
        /// <param name="protocolVersion"></param>
        /// <returns></returns>
        public override string Deserialize(byte[] data, byte protocolVersion)
        {
            return Encoding.ASCII.GetString(data);
        }
    }
}