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

using System.Data;
using System.Text;

namespace CqlSharp.Serialization.Marshal
{
    public class Int32Type : CqlType<int>
    {
        public static readonly Int32Type Instance = new Int32Type();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Int; }
        }

        public override void AppendTypeName(StringBuilder builder)
        {
            builder.Append("org.apache.cassandra.db.marshal.Int32Type");
        }

        /// <summary>
        /// Gets the maximum size in bytes of values of this type.
        /// </summary>
        /// <value>
        /// The maximum size in bytes.
        /// </value>
        public override int Size
        {
            get { return 4; }
        }

        public override DbType ToDbType()
        {
            return DbType.Int32;
        }

        public override byte[] Serialize(int value, byte protocolVersion)
        {
            var data = new byte[4];
            value.ToBytes(data);
            return data;
        }

        public override int Deserialize(byte[] data, byte protocolVersion)
        {
            return data.ToInt();
        }
    }
}