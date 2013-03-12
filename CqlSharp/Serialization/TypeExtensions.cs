// CqlSharp - CqlSharp
// Copyright (c) 2013 Joost Reuzel
//   
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   
// http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace CqlSharp.Serialization
{
    /// <summary>
    ///   Extension methods for the Type class
    /// </summary>
    internal static class TypeExtensions
    {
        /// <summary>
        ///   Determines whether the specified type is anonymous.
        /// </summary>
        /// <param name="type"> The type. </param>
        /// <returns> <c>true</c> if the specified type is anonymous; otherwise, <c>false</c> . </returns>
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