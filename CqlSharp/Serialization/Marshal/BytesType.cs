using System.Data;

namespace CqlSharp.Serialization.Marshal
{
    public class BytesType : CqlType<byte[]>
    {
        public static readonly BytesType Instance = new BytesType();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Blob; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.BytesType"; }
        }

        public override DbType ToDbType()
        {
            return DbType.Binary;
        }

        public override byte[] Serialize(byte[] value)
        {
            return value;
        }

        public override byte[] Deserialize(byte[] data)
        {
            return data;
        }
    }
}