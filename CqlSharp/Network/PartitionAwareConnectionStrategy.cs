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

using System.Linq;
using System.Threading.Tasks;
using CqlSharp.Config;
using CqlSharp.Network.Partition;

namespace CqlSharp.Network
{
    internal class PartitionAwareConnectionStrategy : IConnectionStrategy
    {
        private readonly IConnectionStrategy _baseStrategy;
        private readonly Ring _nodes;

        /// <summary>
        ///   Initializes the strategy with the specified nodes and cluster configuration
        /// </summary>
        /// <param name="nodes"> The nodes. </param>
        /// <param name="config"> The config. </param>
        public PartitionAwareConnectionStrategy(Ring nodes, ClusterConfig config)
        {
            _nodes = nodes;
            _baseStrategy = new BalancedConnectionStrategy(nodes, config);
        }

        #region Implementation of IConnectionStrategy

        /// <summary>
        ///   Gets or creates connection to the cluster.
        /// </summary>
        /// <param name="partitionKey"> </param>
        /// <returns> </returns>
        /// <exception cref="CqlException">Can not connect to any node of the cluster! All connectivity to the cluster seems to be lost</exception>
        public async Task<Connection> GetOrCreateConnectionAsync(PartitionKey partitionKey)
        {
            //try based on partition first
            if (partitionKey.IsSet)
            {
                var nodes = _nodes.GetResponsibleNodes(partitionKey).Where(n => n.IsUp).OrderBy(n => n.Load);

                foreach (Node node in nodes)
                {
                    Connection connection = await node.GetOrCreateConnectionAsync(partitionKey);
                    if (connection != null)
                        return connection;
                }
            }

            return await _baseStrategy.GetOrCreateConnectionAsync(partitionKey);
        }

        /// <summary>
        ///   Invoked when a connection is no longer in use by the application
        /// </summary>
        /// <param name="connection"> The connection no longer used. </param>
        public void ReturnConnection(Connection connection)
        {
        }

        #endregion
    }
}