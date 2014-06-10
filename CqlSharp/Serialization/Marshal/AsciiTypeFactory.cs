using System;

namespace CqlSharp.Serialization.Marshal
{
    public class AsciiTypeFactory : ITypeFactory
    {
        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.AsciiType"; }
        }
        
        public CqlType CreateType(params object[] innerTypes)
        {
            return AsciiType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return AsciiType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return AsciiType.Instance;
        }
    }
}