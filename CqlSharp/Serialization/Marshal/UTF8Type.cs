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
    public class UTF8Type : CqlType<string>
    {
        public static readonly UTF8Type Instance = new UTF8Type();

        private static readonly Type AType = typeof(string);

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Varchar; }
        }

        public override void AppendTypeName(StringBuilder builder)
        {
            builder.Append("org.apache.cassandra.db.marshal.UTF8Type");
        }

        public override Type Type
        {
            get { return AType; }
        }

        public override DbType ToDbType()
        {
            return DbType.String;
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

        public override byte[] Serialize(string value, byte protocolVersion)
        {
            return Encoding.UTF8.GetBytes(value);
        }

        public override string Deserialize(byte[] data, byte protocolVersion)
        {
            return Encoding.UTF8.GetString(data);
        }
    }
}