using System;

namespace CqlSharp.Serialization.Marshal
{
    public class TimeUUIDTypeFactory : ITypeFactory
    {
        public CqlType CreateType(params object[] innerTypes)
        {
            return TimeUUIDType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return TimeUUIDType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return TimeUUIDType.Instance;
        }
    }
}