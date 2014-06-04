using System;

namespace CqlSharp.Serialization.Marshal
{
    public class CounterColumnTypeFactory : ITypeFactory
    {
        public CqlType CreateType(params object[] innerTypes)
        {
            return CounterColumnType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return CounterColumnType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return CounterColumnType.Instance;
        }
    }
}