using System;

namespace CqlSharp.Serialization.Marshal
{
    public class BooleanTypeFactory : ITypeFactory
    {
        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.BooleanType"; }
        }

        public CqlType CreateType(params object[] innerTypes)
        {
            return BooleanType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return BooleanType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return BooleanType.Instance;
        }
    }
}