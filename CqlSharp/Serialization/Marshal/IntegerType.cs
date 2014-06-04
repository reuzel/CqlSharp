using System;
using System.Data;
using System.Numerics;

namespace CqlSharp.Serialization.Marshal
{
    public class IntegerType : CqlType<BigInteger>
    {
        public static readonly IntegerType Instance = new IntegerType();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Varint; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.IntegerType"; }
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