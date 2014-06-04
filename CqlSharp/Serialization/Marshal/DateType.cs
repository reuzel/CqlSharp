using System;
using System.Data;

namespace CqlSharp.Serialization.Marshal
{
    public class DateType : CqlType<DateTime>
    {
        public static readonly DateType Instance = new DateType();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Timestamp; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.DateType"; }
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