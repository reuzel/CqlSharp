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

namespace CqlSharp.Config
{
    /// <summary>
    ///   Strategy options for managing connections to the cluster
    /// </summary>
    public enum ConnectionStrategy
    {
        /// <summary>
        ///   The balanced strategy. Attempts to spread queries over connections based on their load indication
        /// </summary>
        Balanced,

        /// <summary>
        ///   The random strategy. Spreads load, by randomizing access to nodes
        /// </summary>
        Random,

        /// <summary>
        ///   The exclusive strategy. Connections will not be shared between CqlConnection or CqlCommand instances.
        /// </summary>
        Exclusive
    }
}