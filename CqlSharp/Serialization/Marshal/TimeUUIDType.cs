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
    public class TimeUUIDType : CqlType<Guid>
    {
        public static readonly TimeUUIDType Instance = new TimeUUIDType();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Timeuuid; }
        }

        public override void AppendTypeName(StringBuilder builder)
        {
            builder.Append("org.apache.cassandra.db.marshal.TimeUUIDType");
        }

        public override DbType ToDbType()
        {
            return DbType.Guid;
        }


        public override byte[] Serialize(Guid value)
        {
            var data = new byte[16];
            value.ToBytes(data);
            return data;
        }

        public override Guid Deserialize(byte[] data)
        {
            return data.ToGuid();
        }
    }
}