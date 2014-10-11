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

using System;

namespace CqlSharp.Serialization.Marshal
{
    public interface ITypeFactory
    {
        /// <summary>
        /// Gets the name of the type without parameters.
        /// </summary>
        /// <value>
        /// The name of the type.
        /// </value>
        string TypeName { get; }

        /// <summary>
        /// Creates the type based on the provided parameters.
        /// </summary>
        /// <param name="innerTypes">The inner types.</param>
        /// <returns></returns>
        CqlType CreateType(params object[] innerTypes);

        /// <summary>
        /// Creates the type based on a textual represenation of the the type.
        /// </summary>
        /// <param name="parser">The parser.</param>
        /// <returns></returns>
        CqlType CreateType(TypeParser parser);


        /// <summary>
        /// Creates the type based on provided .Net Type
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        CqlType CreateType(Type type);
    }
}