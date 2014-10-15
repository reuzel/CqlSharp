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

namespace CqlSharp
{
    /// <summary>
    /// Defines the scope of the nodes to be discovered when connection to the cluster
    /// </summary>
    public enum DiscoveryScope
    {
        /// <summary>
        /// Find all nodes in the Cassandra cluster
        /// </summary>
        Cluster,

        /// <summary>
        /// Find all nodes in the racks that the configured nodes are part of
        /// </summary>
        Rack,

        /// <summary>
        /// Find all nodes in the datacenters that the configured nodes are part of
        /// </summary>
        DataCenter,

        /// <summary>
        /// Do not search for additional nodes
        /// </summary>
        None
    }
}