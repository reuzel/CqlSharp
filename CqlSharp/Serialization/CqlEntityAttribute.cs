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

namespace CqlSharp.Serialization
{
    /// <summary>
    /// Annotates a class to have it map to a specific name and optionally keyspace
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class CqlEntityAttribute : Attribute
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CqlEntityAttribute" /> class.
        /// </summary>
        /// <param name="name">The name.</param>
        public CqlEntityAttribute(string name)
        {
            Name = name;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlEntityAttribute" /> class.
        /// </summary>
        /// <param name="keySpace">The key space.</param>
        /// <param name="name">The name.</param>
        public CqlEntityAttribute(string keySpace, string name)
        {
            Name = name;
            Keyspace = keySpace;
        }

        /// <summary>
        /// Gets the name.
        /// </summary>
        /// <value>
        /// The name.
        /// </value>
        public string Name { get; private set; }

        /// <summary>
        /// Gets or sets the keyspace.
        /// </summary>
        /// <value>
        /// The keyspace.
        /// </value>
        public string Keyspace { get; set; }
    }
}