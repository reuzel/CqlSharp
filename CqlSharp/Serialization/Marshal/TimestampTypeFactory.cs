using System;

namespace CqlSharp.Serialization.Marshal
{
    public class TimestampTypeFactory : ITypeFactory
    {
        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.TimestampType"; }
        }

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