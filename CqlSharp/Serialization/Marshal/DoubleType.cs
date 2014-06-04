using System;
using System.Data;

namespace CqlSharp.Serialization.Marshal
{
    public class DoubleType : CqlType<double>
    {
        public static readonly DoubleType Instance = new DoubleType();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Double; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.DoubleType"; }
        }

        public override DbType ToDbType()
        {
            return DbType.Double;
        }

        public override byte[] Serialize(double value)
        {
            var data = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian) Array.Reverse(data);
            return data;
        }

        public override double Deserialize(byte[] data)
        {
            if (BitConverter.IsLittleEndian) Array.Reverse(data);
            return BitConverter.ToDouble(data, 0);
        }
    }
}