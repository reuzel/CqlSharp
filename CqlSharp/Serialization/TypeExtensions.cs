using System;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace CqlSharp.Serialization
{
    /// <summary>
    /// Extension methods for the Type class
    /// </summary>
    internal static class TypeExtensions
    {
        /// <summary>
        /// Determines whether the specified type is anonymous.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if the specified type is anonymous; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsAnonymous(this Type type)
        {
            return Attribute.IsDefined(type, typeof (CompilerGeneratedAttribute), false)
                   && type.IsGenericType && type.Name.Contains("AnonymousType")
                   &&
                   (type.Name.StartsWith("<>", StringComparison.OrdinalIgnoreCase) ||
                    type.Name.StartsWith("VB$", StringComparison.OrdinalIgnoreCase))
                   && (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
        }
    }
}