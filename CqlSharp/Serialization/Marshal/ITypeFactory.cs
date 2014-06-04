using System;

namespace CqlSharp.Serialization.Marshal
{
    public interface ITypeFactory
    {
        CqlType CreateType(params object[] innerTypes);
        CqlType CreateType(TypeParser parser);
        CqlType CreateType(Type type);
    }
}