﻿// CqlSharp - CqlSharp
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

namespace CqlSharp.Serialization
{
    /// <summary>
    /// Annotates a class indicating that it should be handled as if it is a user type
    /// </summary>
    public class CqlUserTypeAttribute : CqlEntityAttribute
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CqlUserTypeAttribute" /> class.
        /// </summary>
        /// <param name="keyspace">The keyspace.</param>
        /// <param name="typeName">Name of the type.</param>
        public CqlUserTypeAttribute(string keyspace, string typeName) : base(keyspace, typeName)
        {
        }
    }
}