using System;

namespace CqlSharp.Serialization.Marshal
{
    public class BytesTypeFactory : ITypeFactory
    {
        public CqlType CreateType(params object[] innerTypes)
        {
            return BytesType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return BytesType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return BytesType.Instance;
        }
    }
}