using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using CqlSharp.Protocol;

namespace CqlSharp.Serialization.Marshal
{
    public class SetType<T> : CqlType<HashSet<T>>
    {
        private readonly CqlType<T> _valueType;

        public SetType(CqlType valueType)
        {
            _valueType = (CqlType<T>)valueType;
        }

        public CqlType ValueType
        {
            get { return _valueType; }
        }

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Set; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.SetType(" + _valueType.TypeName + ")"; }
        }

        public override Type Type
        {
            get { return typeof(List<T>); }
        }

        public override DbType ToDbType()
        {
            return DbType.Object;
        }

        public override byte[] Serialize(HashSet<T> value)
        {
            using (var ms = new MemoryStream())
            {
                //write length placeholder
                ms.Position = 2;
                ushort count = 0;
                foreach (T elem in value)
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

        public override HashSet<T> Deserialize(byte[] data)
        {

            using (var ms = new MemoryStream(data))
            {
                ushort nbElem = ms.ReadShort();
                var set = new HashSet<T>();
                for (int i = 0; i < nbElem; i++)
                {
                    byte[] elemRawData = ms.ReadShortByteArray();
                    T elem = _valueType.Deserialize(elemRawData);
                    set.Add(elem);
                }
                return set;
            }
        }

        public override bool Equals(CqlType other)
        {
            var setType = other as SetType<T>;
            return setType != null && setType._valueType.Equals(_valueType);
        }

        public override string ToString()
        {
            return string.Format("set<{0}>", _valueType);
        }
    }
}