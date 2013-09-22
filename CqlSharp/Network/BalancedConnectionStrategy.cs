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
using System.Threading;

namespace CqlSharp.Network
{
    /// <summary>
    ///   This implementation attempts to balance the connections over the cluster based on load. First it will
    ///   try to reuse an existing connection. If no connections exist, or if all connection loads are larger than
    ///   the newConnectionTreshold, a new connection is created at a node with an as low as possible load. If that fails
    ///   (e.g. because the max amount of connections per node is reached), an attempt is made to select the least used
    ///   connection from the least used node.
    /// </summary>
    internal class BalancedConnectionStrategy : IConnectionStrategy
    {
        private readonly CqlConnectionStringBuilder _config;
        private readonly Ring _nodes;
        private int _connectionCount;

        /// <summary>
        ///   Initializes the strategy with the specified nodes and cluster configuration
        /// </summary>
        /// <param name="nodes"> The nodes. </param>
        /// <param name="config"> The config. </param>
        public BalancedConnectionStrategy(Ring nodes, CqlConnectionStringBuilder config)
        {
            _nodes = nodes;
            _config = config;
            _connectionCount = 0;
        }

        #region IConnectionStrategy Members

        /// <summary>
        ///   Gets or creates connection to the cluster.
        /// </summary>
        /// <param name="scope"> </param>
        /// <param name="partitionKey"> </param>
        /// <returns> </returns>
        public Connection GetOrCreateConnection(ConnectionScope scope, PartitionKey partitionKey)
        {
            //provide connections on command level only
            if (scope == ConnectionScope.Connection)
                return null;

            //Sort the nodes by load (used first)
            Node leastUsedNode =
                _nodes.Where(n => n.IsUp).SmallestOrDefault(
                    n => n.ConnectionCount > 0 ? n.Load : _config.NewConnectionTreshold - 1);

            //no node found! weird...
            if (leastUsedNode == null)
                return null;

            //try get a connection from it
            Connection connection = leastUsedNode.GetConnection();

            //smallest connection from smallest node
            if (connection != null && connection.Load < _config.NewConnectionTreshold)
                return connection;

            if (_config.MaxConnections <= 0 || _connectionCount < _config.MaxConnections)
            {
                //try to get a new connection from this smallest node
                Connection newConnection = leastUsedNode.CreateConnection();

                if (newConnection != null)
                {
                    Interlocked.Increment(ref _connectionCount);
                    newConnection.OnConnectionChange +=
                        (c, ev) => { if (ev.Connected == false) Interlocked.Decrement(ref _connectionCount); };

                    return newConnection;
                }
            }

            return connection;
        }


        /// <summary>
        ///   Invoked when a connection is no longer in use by the application
        /// </summary>
        /// <param name="connection"> The connection no longer used. </param>
        public void ReturnConnection(Connection connection, ConnectionScope scope)
        {
            //connections are shared, nothing to do here
        }

        /// <summary>
        ///   Gets a value indicating whether [provide exclusive connections].
        /// </summary>
        /// <value> <c>true</c> if [provide exclusive connections]; otherwise, <c>false</c> . </value>
        public bool ProvidesExclusiveConnections
        {
            get { return false; }
        }

        #endregion
    }
}