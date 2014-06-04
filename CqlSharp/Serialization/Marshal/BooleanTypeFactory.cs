using System;

namespace CqlSharp.Serialization.Marshal
{
    public class BooleanTypeFactory : ITypeFactory
    {
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