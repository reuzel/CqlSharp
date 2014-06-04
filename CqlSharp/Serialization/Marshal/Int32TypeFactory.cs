using System;

namespace CqlSharp.Serialization.Marshal
{
    public class Int32TypeFactory : ITypeFactory
    {
        public CqlType CreateType(params object[] innerTypes)
        {
            return Int32Type.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return Int32Type.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return Int32Type.Instance;
        }
    }
}