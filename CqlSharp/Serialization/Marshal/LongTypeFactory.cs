using System;

namespace CqlSharp.Serialization.Marshal
{
    public class LongTypeFactory : ITypeFactory
    {

        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.LongType"; }
        }

        public CqlType CreateType(params object[] innerTypes)
        {
            return LongType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return LongType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return LongType.Instance;
        }
    }
}