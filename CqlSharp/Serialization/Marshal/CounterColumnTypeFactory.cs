using System;

namespace CqlSharp.Serialization.Marshal
{
    public class CounterColumnTypeFactory : ITypeFactory
    {
        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.CounterColumnType"; }
        }

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