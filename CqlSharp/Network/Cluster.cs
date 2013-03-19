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

using CqlSharp.Config;
using CqlSharp.Network.Partition;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp.Network
{
    /// <summary>
    ///   Represents a Cassandra cluster
    /// </summary>
    internal class Cluster : IConnectionProvider
    {
        private readonly ClusterConfig _config;
        private readonly IConnectionStrategy _connectionSelector;
        private Ring _nodes;
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
            BuildRing();

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
                case ConnectionStrategy.PartitionAware:
                    _connectionSelector = new PartitionAwareConnectionStrategy(_nodes, _config);
                    break;
            }

            //setup throttle
            int concurrent = config.MaxConcurrentQueries <= 0
                                 ? _nodes.Count * config.MaxConnectionsPerNode * 256
                                 : config.MaxConcurrentQueries;

            _throttle = new SemaphoreSlim(concurrent);
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
        /// <param name="partitionKey"> </param>
        /// <returns> </returns>
        public Task<Connection> GetOrCreateConnectionAsync(PartitionKey partitionKey)
        {
            return _connectionSelector.GetOrCreateConnectionAsync(partitionKey);
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
        ///   Finds additional nodes to connect to.
        /// </summary>
        /// <exception cref="CqlException">Could not detect datacenter or rack information from the node specified in the config section!</exception>
        private void BuildRing()
        {
            //get the first set of seed-nodes
            var nodes = CreateNodesFromConfig();

            //the partitioner in use
            string partitioner;

            //create list of nodes of which info is found
            var found = new List<Node>();

            using (var connection = new CqlConnection(nodes[0]))
            {
                var cmd = new CqlCommand(connection, "select partitioner from system.local", CqlConsistency.One);
                partitioner = (string)cmd.ExecuteScalar();

                foreach (Node node in nodes)
                {
                    //if node is part of already found list, skip...
                    if (found.Contains(node))
                        continue;

                    //get the "local" data center, rack and token
                    var localcmd = new CqlCommand(connection, "select data_center, rack, tokens from system.local",
                                                  CqlConsistency.One);

                    using (CqlDataReader reader = localcmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            node.DataCenter = (string)reader["data_center"];
                            node.Rack = (string)reader["rack"];
                            node.Tokens = (ISet<string>)reader["tokens"];
                        }
                    }

                    //make sure we actually found a datacenter or rack
                    if (node.DataCenter == null || node.Rack == null)
                        throw new CqlException(
                            "Could not detect datacenter or rack information from the node specified in the config section!");

                    //add this node, now info is complete
                    found.Add(node);

                    //get the peers
                    var peerscmd = new CqlCommand(connection,
                                                  "select rpc_address, data_center, rack, tokens from system.peers",
                                                  CqlConsistency.One);

                    using (CqlDataReader reader = peerscmd.ExecuteReader())
                    {
                        //iterate over the peers
                        while (reader.Read())
                        {
                            var newNode = new Node((IPAddress)reader["rpc_address"], _config)
                                              {
                                                  DataCenter = (string)reader["data_center"],
                                                  Rack = (string)reader["rack"],
                                                  Tokens = (ISet<string>)reader["tokens"]
                                              };

                            //filter based on scope
                            switch (_config.DiscoveryScope)
                            {
                                case DiscoveryScope.None:
                                    //skip if item is not in configured list
                                    if (!nodes.Contains(newNode))
                                        continue;

                                    break;

                                case DiscoveryScope.DataCenter:
                                    //skip if node does not match datacenter
                                    if (
                                        !node.DataCenter.Equals(newNode.DataCenter,
                                                                StringComparison.InvariantCultureIgnoreCase))
                                        continue;
                                    break;

                                case DiscoveryScope.Rack:
                                    //skip if node does not match datacenter
                                    if (
                                        !node.DataCenter.Equals(newNode.DataCenter,
                                                                StringComparison.InvariantCultureIgnoreCase))
                                        continue;
                                    //skipt if node does not match rack
                                    if (!node.Rack.Equals(newNode.Rack, StringComparison.InvariantCultureIgnoreCase))
                                        continue;
                                    break;
                            }

                            //add the node if we did not find it yet
                            if (!found.Contains(newNode))
                            {
                                found.Add(newNode);
                            }
                        }
                    }
                }
            }

            _nodes = new Ring(found, partitioner);
        }

        /// <summary>
        ///   Creates the nodes from based on the servers value from the config.
        /// </summary>
        private List<Node> CreateNodesFromConfig()
        {
            var nodes = new List<Node>();

            foreach (string nameOrAddress in _config.Nodes)
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

                    var node = new Node(address, _config);
                    nodes.Add(node);
                }
                catch (Exception ex)
                {
                    throw new CqlException("Can not obtain a valid IP-Address from the nodes specified in the configuration", ex);
                }
            }

            return nodes;
        }
    }
}