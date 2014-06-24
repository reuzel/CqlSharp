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
using System.Numerics;
using System.Text;

namespace CqlSharp.Serialization.Marshal
{
    public class IntegerType : CqlType<BigInteger>
    {
        public static readonly IntegerType Instance = new IntegerType();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Varint; }
        }

        public override void AppendTypeName(StringBuilder builder)
        {
            builder.Append("org.apache.cassandra.db.marshal.IntegerType");
        }

        public override DbType ToDbType()
        {
            return DbType.VarNumeric;
        }

        public override byte[] Serialize(BigInteger value)
        {
            var data = value.ToByteArray();
            Array.Reverse(data); //to big endian
            return data;
        }

        public override BigInteger Deserialize(byte[] data)
        {
            //to little endian
            Array.Reverse(data);
            return new BigInteger(data);
        }
    }
}