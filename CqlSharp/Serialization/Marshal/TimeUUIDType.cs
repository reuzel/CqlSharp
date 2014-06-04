using System;
using System.Data;

namespace CqlSharp.Serialization.Marshal
{
    public class TimeUUIDType : CqlType<Guid>
    {
        public static readonly TimeUUIDType Instance = new TimeUUIDType();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Timeuuid; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.TimeUUIDType"; }
        }

        public override DbType ToDbType()
        {
            return DbType.Guid;
        }


        public override byte[] Serialize(Guid value)
        {
            var data = new byte[16];
            value.ToBytes(data);
            return data;
        }

        public override Guid Deserialize(byte[] data)
        {
            return data.ToGuid();
        }
    }
}