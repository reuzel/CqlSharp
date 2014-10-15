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
    public class DoubleType : CqlType<double>
    {
        public static readonly DoubleType Instance = new DoubleType();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Double; }
        }

        public override void AppendTypeName(StringBuilder builder)
        {
            builder.Append("org.apache.cassandra.db.marshal.DoubleType");
        }

        public override DbType ToDbType()
        {
            return DbType.Double;
        }

        /// <summary>
        /// Gets the maximum size in bytes of values of this type.
        /// </summary>
        /// <value>
        /// The maximum size in bytes.
        /// </value>
        public override int Size
        {
            get { return 8; }
        }

        public override byte[] Serialize(double value, byte protocolVersion)
        {
            var data = BitConverter.GetBytes(value);
            if(BitConverter.IsLittleEndian) Array.Reverse(data);
            return data;
        }

        public override double Deserialize(byte[] data, byte protocolVersion)
        {
            if(BitConverter.IsLittleEndian) Array.Reverse(data);
            return BitConverter.ToDouble(data, 0);
        }
    }
}