using System;
using System.Data;

namespace CqlSharp.Serialization.Marshal
{
    public class FloatType : CqlType<float>
    {
        public static readonly FloatType Instance = new FloatType();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Float; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.FloatType"; }
        }

        public override DbType ToDbType()
        {
            return DbType.Single;
        }

        public override byte[] Serialize(float value)
        {
            var data = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian) Array.Reverse(data);
            return data;
        }

        public override float Deserialize(byte[] data)
        {
            if (BitConverter.IsLittleEndian) Array.Reverse(data);
            return BitConverter.ToSingle(data, 0);
        }
    }
}