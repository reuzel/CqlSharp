using System;
using System.Collections.Concurrent;

namespace CqlSharp.Serialization.Marshal
{
    public class ListTypeFactory : ITypeFactory
    {
        private readonly ConcurrentDictionary<CqlType, CqlType> _types = new ConcurrentDictionary<CqlType, CqlType>();

        public CqlType CreateType(params object[] innerTypes)
        {
            var innerType = innerTypes[0] as CqlType;
            if (innerType == null)
                throw new CqlException("Need a CqlType as parameter when constructing a ListType");

            return _types.GetOrAdd(innerType, type =>
                                              (CqlType)Activator.CreateInstance(typeof(ListType<>).MakeGenericType(type.Type), type)
                );
        }

        public CqlType CreateType(TypeParser parser)
        {
            throw new NotImplementedException();
        }

        public CqlType CreateType(Type type)
        {
            return CreateType(CqlType.CreateType(type.GetGenericArguments()[0]));
        }
    }
}