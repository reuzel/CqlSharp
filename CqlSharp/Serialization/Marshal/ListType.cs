using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
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
            get { return _valueType.CqlTypeCode == CqlTypeCode.Custom ? CqlTypeCode.Custom : CqlTypeCode.List; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.ListType(" + _valueType.TypeName + ")"; }
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

        public override List<T> Deserialize(byte[] data)
        {

            using (var ms = new MemoryStream(data))
            {
                ushort nbElem = ms.ReadShort();
                var list = new List<T>(nbElem);
                for (int i = 0; i < nbElem; i++)
                {
                    byte[] elemRawData = ms.ReadShortByteArray();
                    T elem = _valueType.Deserialize(elemRawData);
                    list.Add(elem);
                }
                return list;
            }
        }

        public override string ToString()
        {
            return string.Format("list<{0}>", _valueType);
        }
    }
}