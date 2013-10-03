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
using System.Linq;

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
        public PartitionAwareConnectionStrategy(Ring nodes, CqlConnectionStringBuilder config)
        {
            _nodes = nodes;
            _baseStrategy = new BalancedConnectionStrategy(nodes, config);
        }

        #region Implementation of IConnectionStrategy

        /// <summary>
        ///   Gets or creates connection to the cluster.
        /// </summary>
        /// <param name="scope"> The scope. </param>
        /// <param name="partitionKey"> The partition key. </param>
        /// <returns> </returns>
        /// <exception cref="CqlException">Can not connect to any node of the cluster! All connectivity to the cluster seems to be lost</exception>
        public Connection GetOrCreateConnection(ConnectionScope scope, PartitionKey partitionKey)
        {
            //provide connections on command level only
            if (scope == ConnectionScope.Connection)
                return null;

            //try based on partition first
            if (partitionKey != null && partitionKey.IsSet)
            {
                var nodes = _nodes.GetResponsibleNodes(partitionKey).Where(n => n.IsUp).OrderBy(n => n.Load);

                foreach (Node node in nodes)
                {
                    Connection connection = node.GetOrCreateConnection(partitionKey);
                    if (connection != null)
                        return connection;
                }
            }

            return _baseStrategy.GetOrCreateConnection(scope, partitionKey);
        }

        /// <summary>
        ///   Invoked when a connection is no longer in use by the application
        /// </summary>
        /// <param name="connection"> The connection no longer used. </param>
        /// <param name="scope"> The scope. </param>
        public void ReturnConnection(Connection connection, ConnectionScope scope)
        {
            _baseStrategy.ReturnConnection(connection, scope);
        }

        public bool ProvidesExclusiveConnections
        {
            get { return false; }
        }

        #endregion
    }
}