using System.Collections.Generic;
using System.Data;
using System.IO;
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
            get
            {
                return CqlTypeCode.Map;
            }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.MapType(" + _keyType.TypeName + "," + _valueType.TypeName + ")"; }
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

        public override byte[] Serialize(Dictionary<TKey, TValue> map)
        {
            using (var ms = new MemoryStream())
            {
                ms.WriteShort((ushort)map.Count);
                foreach (var de in map)
                {
                    byte[] rawDataKey = _keyType.Serialize(de.Key);
                    ms.WriteShortByteArray(rawDataKey);
                    byte[] rawDataValue = _valueType.Serialize(de.Value);
                    ms.WriteShortByteArray(rawDataValue);
                }
                return ms.ToArray();
            }
        }

        public override Dictionary<TKey, TValue> Deserialize(byte[] data)
        {
            using (var ms = new MemoryStream(data))
            {
                ushort nbElem = ms.ReadShort();
                var map = new Dictionary<TKey, TValue>(nbElem);
                for (int i = 0; i < nbElem; i++)
                {
                    byte[] elemRawKey = ms.ReadShortByteArray();
                    byte[] elemRawValue = ms.ReadShortByteArray();
                    TKey key = _keyType.Deserialize(elemRawKey);
                    TValue value = _valueType.Deserialize(elemRawValue);
                    map.Add(key, value);
                }
                return map;
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