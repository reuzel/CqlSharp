using System;

namespace CqlSharp.Serialization.Marshal
{
    public class TimestampTypeFactory : ITypeFactory
    {
        public CqlType CreateType(params object[] innerTypes)
        {
            return TimestampType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return TimestampType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return TimestampType.Instance;
        }
    }
}