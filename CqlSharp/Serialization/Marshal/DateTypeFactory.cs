using System;

namespace CqlSharp.Serialization.Marshal
{
    public class DateTypeFactory : ITypeFactory
    {

        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.DateType"; }
        }

        public CqlType CreateType(params object[] innerTypes)
        {
            return DateType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return DateType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return LongType.Instance;
        }
    }
}