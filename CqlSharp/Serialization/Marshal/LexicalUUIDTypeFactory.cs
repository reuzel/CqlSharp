using System;

namespace CqlSharp.Serialization.Marshal
{
    public class LexicalUUIDTypeFactory : ITypeFactory
    {
        public CqlType CreateType(params object[] innerTypes)
        {
            return LexicalUUIDType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return LexicalUUIDType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return LexicalUUIDType.Instance;
        }
    }
}