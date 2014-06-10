using System;

namespace CqlSharp.Serialization.Marshal
{
    public interface ITypeFactory
    {
        string TypeName { get; }

        CqlType CreateType(params object[] innerTypes);
        CqlType CreateType(TypeParser parser);
        CqlType CreateType(Type type);
    }
}