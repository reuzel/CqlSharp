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

using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using CqlSharp.Config;

namespace CqlSharp.Network
{
    /// <summary>
    ///   Represents a Cassandra cluster
    /// </summary>
    internal class Cluster : IConnectionProvider
    {
        private readonly ClusterConfig _config;
        private readonly IConnectionStrategy _connectionSelector;
        private readonly List<Node> _nodes;
        private readonly SemaphoreSlim _throttle;

        /// <summary>
        ///   Initializes a new instance of the <see cref="Cluster" /> class.
        /// </summary>
        /// <param name="config"> The config. </param>
        public Cluster(ClusterConfig config)
        {
            //store config
            _config = config;

            //setup nodes
            _nodes = new List<Node>();
            CreateNodesFromConfig(config);
            NodeDiscovery.ExpandNodeList(_nodes, config);

            //setup cluster connection strategy
            switch (_config.ConnectionStrategy)
            {
                case ConnectionStrategy.Balanced:
                    _connectionSelector = new BalancedConnectionStrategy(_nodes, config);
                    break;
                case ConnectionStrategy.Random:
                    _connectionSelector = new RandomConnectionStrategy(_nodes, config);
                    break;
                case ConnectionStrategy.Exclusive:
                    _connectionSelector = new ExclusiveConnectionStrategy(_nodes, _config);
                    break;
            }

            //setup throttle
            if (config.MaxConcurrentQueries <= 0)
                config.MaxConcurrentQueries = _nodes.Count*config.MaxConnectionsPerNode*256;

            _throttle = new SemaphoreSlim(config.MaxConcurrentQueries);
        }

        /// <summary>
        ///   Gets the throttle to limit concurrent requests.
        /// </summary>
        /// <value> The throttle. </value>
        public SemaphoreSlim Throttle
        {
            get { return _throttle; }
        }

        #region IConnectionProvider Members

        /// <summary>
        ///   Gets a connection to a node in the cluster
        /// </summary>
        /// <returns> </returns>
        public Task<Connection> GetOrCreateConnectionAsync()
        {
            return _connectionSelector.GetOrCreateConnectionAsync();
        }

        /// <summary>
        ///   Returns the connection.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        public void ReturnConnection(Connection connection)
        {
            _connectionSelector.ReturnConnection(connection);
        }

        #endregion

        /// <summary>
        ///   Creates the nodes from based on the servers value from the config.
        /// </summary>
        /// <param name="config"> The config. </param>
        private void CreateNodesFromConfig(ClusterConfig config)
        {
            foreach (string nameOrAddress in config.Nodes)
            {
                try
                {
                    IPAddress address;
                    if (!IPAddress.TryParse(nameOrAddress, out address))
                    {
                        address =
                            Dns.GetHostAddresses(nameOrAddress).FirstOrDefault(
                                addr => addr.AddressFamily == AddressFamily.InterNetwork);
                    }

                    var node = new Node(address, config);
                    _nodes.Add(node);
                }
                catch
                {
                    continue;
                }
            }
        }
    }
}