using System;
using System.Collections.Concurrent;

namespace CqlSharp.Serialization.Marshal
{
    public class SetTypeFactory : ITypeFactory
    {
        private static readonly ConcurrentDictionary<CqlType, CqlType> _types = new ConcurrentDictionary<CqlType, CqlType>();

        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.SetType"; }
        }
        
        public CqlType CreateType(params object[] innerTypes)
        {
            var innerType = innerTypes[0] as CqlType;
            if (innerType == null)
                throw new CqlException("Need a CqlType as parameter when constructing a SetType");

            return CreateType(innerType);

        }

        public CqlType CreateType(CqlType innerType)
        {
            return _types.GetOrAdd(innerType, type =>
                                              (CqlType)Activator.CreateInstance(typeof(SetType<>).MakeGenericType(type.Type), type)
                );
        }

        public CqlType CreateType(TypeParser parser)
        {
            var innerType = parser.ReadCqlType();
            return CreateType(innerType);
        }

        public CqlType CreateType(Type type)
        {
            return CreateType(CqlType.CreateType(type.GetGenericArguments()[0]));
        }
    }
}