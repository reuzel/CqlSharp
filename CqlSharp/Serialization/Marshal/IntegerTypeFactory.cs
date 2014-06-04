using System;

namespace CqlSharp.Serialization.Marshal
{
    public class IntegerTypeFactory : ITypeFactory
    {
        public CqlType CreateType(params object[] innerTypes)
        {
            return IntegerType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return IntegerType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return IntegerType.Instance;
        }
    }
}