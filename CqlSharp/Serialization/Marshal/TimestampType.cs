using System;
using System.Data;

namespace CqlSharp.Serialization.Marshal
{
    public class TimestampType : CqlType<DateTime>
    {
        public static readonly TimestampType Instance = new TimestampType();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Timestamp; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.TimestampType"; }
        }

        public override DbType ToDbType()
        {
            return DbType.DateTime;
        }

        public override byte[] Serialize(DateTime value)
        {
            var data = new byte[8];
            value.ToBytes(data);
            return data;
        }

        public override DateTime Deserialize(byte[] data)
        {
            return data.ToLong().ToDateTime();
        }
    }
}