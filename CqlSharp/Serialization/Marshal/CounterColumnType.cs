using System.Data;

namespace CqlSharp.Serialization.Marshal
{
    public class CounterColumnType : CqlType<long>
    {
        public static readonly CounterColumnType Instance = new CounterColumnType();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Counter; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.CounterColumnType"; }
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