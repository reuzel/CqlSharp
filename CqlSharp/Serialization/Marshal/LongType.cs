using System.Data;

namespace CqlSharp.Serialization.Marshal
{
    public class LongType : CqlType<long>
    {
        public static readonly LongType Instance = new LongType();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Bigint; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.LongType"; }
        }

        public override DbType ToDbType()
        {
            return DbType.Int64;
        }

        public override byte[] Serialize(long value)
        {

            var data = new byte[8];
            value.ToBytes(data);
            return data;
        }

        public override long Deserialize(byte[] data)
        {
            return data.ToLong();
        }
    }
}