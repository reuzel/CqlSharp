using System;

namespace CqlSharp.Serialization.Marshal
{
    public class FloatTypeFactory : ITypeFactory
    {
        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.FloatType"; }
        }

        public CqlType CreateType(params object[] innerTypes)
        {
            return FloatType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return FloatType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return FloatType.Instance;
        }
    }
}