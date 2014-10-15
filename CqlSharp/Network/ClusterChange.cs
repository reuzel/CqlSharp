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

namespace CqlSharp.Network
{
    /// <summary>
    /// CqlTypeCode of possible evented changes to a cluster
    /// </summary>
    internal enum ClusterChange
    {
        /// <summary>
        /// a node was restored to active duty
        /// </summary>
        Up,

        /// <summary>
        /// A node become unavailable
        /// </summary>
        Down,

        /// <summary>
        /// A node was added to the cluster
        /// </summary>
        New,

        /// <summary>
        /// A node was (permanently) removed from the cluster
        /// </summary>
        Removed
    }
}