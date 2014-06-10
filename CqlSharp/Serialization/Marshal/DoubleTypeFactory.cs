using System;

namespace CqlSharp.Serialization.Marshal
{
    public class DoubleTypeFactory : ITypeFactory
    {
        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.DoubleType"; }
        }

        public CqlType CreateType(params object[] innerTypes)
        {
            return DoubleType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return DoubleType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return DoubleType.Instance;
        }
    }
}