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

using System.Reflection;

namespace CqlSharp.Serialization
{
    /// <summary>
    /// Utility class to enable setting of key flags
    /// </summary>
    internal interface IKeyMember
    {
        /// <summary>
        /// Gets the member information.
        /// </summary>
        /// <value> The member information. </value>
        MemberInfo MemberInfo { get; }

        /// <summary>
        /// Gets or sets a value indicating whether this instance is partition key.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is partition key; otherwise, <c>false</c>.
        /// </value>
        bool IsPartitionKey { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this instance is clustering key.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is clustering key; otherwise, <c>false</c>.
        /// </value>
        bool IsClusteringKey { get; set; }
    }
}