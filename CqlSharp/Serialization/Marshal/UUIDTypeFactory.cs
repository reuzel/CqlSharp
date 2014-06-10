using System;

namespace CqlSharp.Serialization.Marshal
{
    public class UUIDTypeFactory : ITypeFactory
    {
        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.UUIDType"; }
        }

        public CqlType CreateType(params object[] innerTypes)
        {
            return UUIDType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return UUIDType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return UUIDType.Instance;
        }
    }
}