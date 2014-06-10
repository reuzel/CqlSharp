using System;

namespace CqlSharp.Serialization.Marshal
{
    public class InetAddressTypeFactory : ITypeFactory
    {
        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.InetAddressType"; }
        }

        public CqlType CreateType(params object[] innerTypes)
        {
            return InetAddressType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return InetAddressType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return InetAddressType.Instance;
        }
    }
}