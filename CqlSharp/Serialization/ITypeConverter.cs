// CqlSharp - CqlSharp
// Copyright (c) 2014 Joost Reuzel
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

using CqlSharp.Annotations;

namespace CqlSharp.Serialization
{
    /// <summary>
    /// Defines conversions from an to the given type
    /// </summary>
    /// <typeparam name="TType">The type of the type.</typeparam>
    public interface ITypeConverter<TType>
    {
        /// <summary>
        /// Converts the source object to an object of the the given target type.
        /// </summary>
        /// <typeparam name="TTarget">The type of the target.</typeparam>
        /// <param name="source">The source.</param>
        /// <returns>an object of the the given target type</returns>
        [UsedImplicitly]
        TTarget ConvertTo<TTarget>(TType source);

        /// <summary>
        /// Converts an object of the given source type to an instance of this converters type
        /// </summary>
        /// <typeparam name="TSource">The type of the source.</typeparam>
        /// <param name="source">The source.</param>
        /// <returns></returns>
        [UsedImplicitly]
        TType ConvertFrom<TSource>(TSource source);
    }
}