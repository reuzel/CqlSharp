using System;

namespace CqlSharp.Serialization.Marshal
{
    public class LexicalUUIDTypeFactory : ITypeFactory
    {
        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.LexicalUUIDType"; }
        }

        public CqlType CreateType(params object[] innerTypes)
        {
            return LexicalUUIDType.Instance;
        }

        public CqlType CreateType(TypeParser parser)
        {
            return LexicalUUIDType.Instance;
        }

        public CqlType CreateType(Type type)
        {
            return LexicalUUIDType.Instance;
        }
    }
}