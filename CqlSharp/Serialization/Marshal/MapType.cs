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

using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using CqlSharp.Protocol;

namespace CqlSharp.Serialization.Marshal
{
    public class MapType<TKey, TValue> : CqlType<Dictionary<TKey, TValue>>
    {
        private readonly CqlType<TKey> _keyType;
        private readonly CqlType<TValue> _valueType;

        public MapType(CqlType keyType, CqlType valueType)
        {
            _keyType = (CqlType<TKey>)keyType;
            _valueType = (CqlType<TValue>)valueType;
        }

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Map; }
        }

        public override void AppendTypeName(StringBuilder builder)
        {
            builder.Append("org.apache.cassandra.db.marshal.MapType(");
            _keyType.AppendTypeName(builder);
            builder.Append(",");
            _valueType.AppendTypeName(builder);
            builder.Append(")");
        }

        public CqlType KeyType
        {
            get { return _keyType; }
        }

        public CqlType ValueType
        {
            get { return _valueType; }
        }

        public override DbType ToDbType()
        {
            return DbType.Object;
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

        public override byte[] Serialize(Dictionary<TKey, TValue> map, byte protocolVersion)
        {
            if(protocolVersion <= 2)
            {
                using(var ms = new MemoryStream())
                {
                    ms.WriteShort((ushort)map.Count);
                    foreach(var de in map)
                    {
                        byte[] rawDataKey = _keyType.Serialize(de.Key, protocolVersion);
                        ms.WriteShortByteArray(rawDataKey);
                        byte[] rawDataValue = _valueType.Serialize(de.Value, protocolVersion);
                        ms.WriteShortByteArray(rawDataValue);
                    }
                    return ms.ToArray();
                }
            }
            else
            {
                using (var ms = new MemoryStream())
                {
                    ms.WriteInt(map.Count);
                    foreach (var de in map)
                    {
                        byte[] rawDataKey = _keyType.Serialize(de.Key, protocolVersion);
                        ms.WriteByteArray(rawDataKey);
                        byte[] rawDataValue = _valueType.Serialize(de.Value, protocolVersion);
                        ms.WriteByteArray(rawDataValue);
                    }
                    return ms.ToArray();
                }
            }
        }

        public override Dictionary<TKey, TValue> Deserialize(byte[] data, byte protocolVersion)
        {
            if(protocolVersion <= 2)
            {
                using(var ms = new MemoryStream(data))
                {
                    ushort nbElem = ms.ReadShort();
                    var map = new Dictionary<TKey, TValue>(nbElem);
                    for(int i = 0; i < nbElem; i++)
                    {
                        byte[] elemRawKey = ms.ReadShortByteArray();
                        byte[] elemRawValue = ms.ReadShortByteArray();
                        TKey key = _keyType.Deserialize(elemRawKey, protocolVersion);
                        TValue value = _valueType.Deserialize(elemRawValue, protocolVersion);
                        map.Add(key, value);
                    }
                    return map;
                }
            }
            else
            {
                using (var ms = new MemoryStream(data))
                {
                    int nbElem = ms.ReadInt();
                    var map = new Dictionary<TKey, TValue>(nbElem);
                    for (int i = 0; i < nbElem; i++)
                    {
                        byte[] elemRawKey = ms.ReadByteArray();
                        byte[] elemRawValue = ms.ReadByteArray();
                        TKey key = _keyType.Deserialize(elemRawKey, protocolVersion);
                        TValue value = _valueType.Deserialize(elemRawValue, protocolVersion);
                        map.Add(key, value);
                    }
                    return map;
                }
            }
        }

        public override bool Equals(CqlType other)
        {
            var mapType = other as MapType<TKey, TValue>;
            return mapType != null && mapType._keyType.Equals(_keyType) && mapType._valueType.Equals(_valueType);
        }

        public override string ToString()
        {
            return string.Format("map<{0},{1}>", _keyType, _valueType);
        }
    }
}