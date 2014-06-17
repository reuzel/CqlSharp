using System;
namespace CqlSharp.Serialization
{
    interface ICastable
    {
        T CastTo<T>();
    }
}
