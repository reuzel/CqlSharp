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
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using CqlSharp.Protocol;

namespace CqlSharp.Serialization.Marshal
{
    public class ListType<T> : CqlType<List<T>>
    {
        private readonly CqlType<T> _valueType;

        public ListType(CqlType valueType)
        {
            _valueType = (CqlType<T>)valueType;
        }

        public CqlType ValueType
        {
            get { return _valueType; }
        }

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.List; }
        }

        public override void AppendTypeName(StringBuilder builder)
        {
            builder.Append("org.apache.cassandra.db.marshal.ListType(");
            _valueType.AppendTypeName(builder);
            builder.Append(")");
        }

        public override Type Type
        {
            get { return typeof(List<T>); }
        }

        public override DbType ToDbType()
        {
            return DbType.Object;
        }

        public override byte[] Serialize(List<T> value)
        {
            using(var ms = new MemoryStream())
            {
                //write length placeholder
                ms.Position = 2;
                ushort count = 0;
                foreach(T elem in value)
                {
                    byte[] rawDataElem = _valueType.Serialize(elem);
                    ms.WriteShortByteArray(rawDataElem);
                    count++;
                }
                ms.Position = 0;
                ms.WriteShort(count);
                return ms.ToArray();
            }
        }

        public override List<T> Deserialize(byte[] data)
        {
            using(var ms = new MemoryStream(data))
            {
                ushort nbElem = ms.ReadShort();
                var list = new List<T>(nbElem);
                for(int i = 0; i < nbElem; i++)
                {
                    byte[] elemRawData = ms.ReadShortByteArray();
                    T elem = _valueType.Deserialize(elemRawData);
                    list.Add(elem);
                }
                return list;
            }
        }

        public override bool Equals(CqlType other)
        {
            var listType = other as ListType<T>;
            return listType != null && listType._valueType.Equals(_valueType);
        }

        public override string ToString()
        {
            return string.Format("list<{0}>", _valueType);
        }
    }
}