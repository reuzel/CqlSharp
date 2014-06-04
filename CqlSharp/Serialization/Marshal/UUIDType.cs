using System;
using System.Data;

namespace CqlSharp.Serialization.Marshal
{
    public class UUIDType : CqlType<Guid>
    {
        public static readonly UUIDType Instance = new UUIDType();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Uuid; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.UUIDType"; }
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