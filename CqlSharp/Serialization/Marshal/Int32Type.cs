using System.Data;

namespace CqlSharp.Serialization.Marshal
{
    public class Int32Type : CqlType<int>
    {
        public static readonly Int32Type Instance = new Int32Type();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Int; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.Int32Type"; }
        }

        public override DbType ToDbType()
        {
            return DbType.Int32;
        }

        public override byte[] Serialize(int value)
        {
            var data = new byte[4];
            value.ToBytes(data);
            return data;
        }

        public override int Deserialize(byte[] data)
        {
            return data.ToInt();
        }
    }
}