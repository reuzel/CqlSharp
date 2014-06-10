using System;

namespace CqlSharp.Serialization.Marshal
{
    public class UTF8TypeFactory : ITypeFactory
    {
        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.UTF8Type"; }
        }

        public CqlType CreateType(params object[] innerTypes)
        {
            return UTF8Type.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return UTF8Type.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return UTF8Type.Instance;
        }
    }
}