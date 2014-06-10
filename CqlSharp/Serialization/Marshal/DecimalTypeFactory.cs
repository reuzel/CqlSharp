using System;

namespace CqlSharp.Serialization.Marshal
{
    public class DecimalTypeFactory : ITypeFactory
    {
        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.DecimalType"; }
        }
        
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