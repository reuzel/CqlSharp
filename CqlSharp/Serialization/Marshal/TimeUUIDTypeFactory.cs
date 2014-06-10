using System;

namespace CqlSharp.Serialization.Marshal
{
    public class TimeUUIDTypeFactory : ITypeFactory
    {
        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.TimeUUIDType"; }
        }

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