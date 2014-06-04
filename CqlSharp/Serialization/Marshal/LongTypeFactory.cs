using System;

namespace CqlSharp.Serialization.Marshal
{
    public class LongTypeFactory : ITypeFactory
    {
        public CqlType CreateType(params object[] innerTypes)
        {
            return LongType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return LongType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return LongType.Instance;
        }
    }
}