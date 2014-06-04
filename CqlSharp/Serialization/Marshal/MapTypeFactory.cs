using System;
using System.Collections.Concurrent;

namespace CqlSharp.Serialization.Marshal
{
    public class MapTypeFactory : ITypeFactory
    {
        private readonly ConcurrentDictionary<Tuple<CqlType, CqlType>, CqlType> _types = new ConcurrentDictionary<Tuple<CqlType, CqlType>, CqlType>();

        public CqlType CreateType(params object[] innerTypes)
        {
            var keyType = innerTypes[0] as CqlType;
            var valueType = innerTypes[1] as CqlType;

            if (keyType == null || valueType == null)
                throw new CqlException("Need two CqlTypes for key and value as parameters when constructing a MapType");

            var tuple = new Tuple<CqlType, CqlType>(keyType, valueType);

            return _types.GetOrAdd(tuple, types =>
                                          (CqlType)Activator.CreateInstance(
                                              typeof(MapType<,>).MakeGenericType(types.Item1.Type, types.Item2.Type),
                                              types.Item1,
                                              types.Item2));
        }

        public CqlType CreateType(TypeParser parser)
        {
            throw new NotImplementedException();
        }

        public CqlType CreateType(Type type)
        {
            var typeArgs = type.GetGenericArguments();
            return CreateType(CqlType.CreateType(typeArgs[0]), CqlType.CreateType(typeArgs[1]));
        }
    }
}