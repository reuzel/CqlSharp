using System;

namespace CqlSharp.Serialization.Marshal
{
    public class DecimalTypeFactory : ITypeFactory
    {
        public CqlType CreateType(params object[] innerTypes)
        {
            return DecimalType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return DecimalType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return DecimalType.Instance;
        }
    }
}