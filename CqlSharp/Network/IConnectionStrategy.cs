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

using CqlSharp.Network.Partition;

namespace CqlSharp.Network
{
    internal interface IConnectionStrategy
    {
        /// <summary>
        ///   Gets or creates connection to the cluster.
        /// </summary>
        /// <param name="partitionKey"> </param>
        /// <returns> </returns>
        /// <exception cref="CqlException">Can not connect to any node of the cluster! All connectivity to the cluster seems to be lost</exception>
        Connection GetOrCreateConnection(PartitionKey partitionKey);

        /// <summary>
        ///   Invoked when a connection is no longer in use by the application
        /// </summary>
        /// <param name="connection"> The connection no longer used. </param>
        void ReturnConnection(Connection connection);
    }
}