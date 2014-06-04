using System.Data;
using System.Net;

namespace CqlSharp.Serialization.Marshal
{
    public class InetAddressType : CqlType<IPAddress>
    {
        public static readonly InetAddressType Instance = new InetAddressType();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Inet; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.InetAddressType"; }
        }

        public override DbType ToDbType()
        {
            return DbType.Object;
        }

        public override byte[] Serialize(IPAddress value)
        {
            return value.GetAddressBytes();
        }

        public override IPAddress Deserialize(byte[] data)
        {
            return new IPAddress(data);
        }
    }
}