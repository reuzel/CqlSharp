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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using CqlSharp.Config;

namespace CqlSharp.Network
{
    /// <summary>
    ///   Discovers (additional) nodes in the cluster to which connections can be made
    /// </summary>
    internal class NodeDiscovery
    {
        /// <summary>
        ///   Finds additional nodes to connect to.
        /// </summary>
        /// <param name="nodes"> the list of nodes to expand </param>
        /// <param name="config"> The config. </param>
        /// <returns> a list of nodes found </returns>
        /// <exception cref="CqlException">Could not detect datacenter or rack information from the node specified in the config section!</exception>
        public static void ExpandNodeList(List<Node> nodes, ClusterConfig config)
        {
            if (config.DiscoveryScope.Equals(DiscoveryScope.None))
                return;

            var found = new List<Node>();

            foreach (Node node in nodes)
            {
                //if node is part of already found list, skip...
                if (found.Any(n => n.Address.Equals(node.Address)))
                    continue;

                //create a connection to the node
                using (var connection = new CqlConnection(node))
                {
                    string datacenter = null, rack = null;

                    //get the "local" data center and rack
                    var localcmd = new CqlCommand(connection, "select data_center, rack from system.local",
                                                  CqlConsistency.Any);
                    using (CqlDataReader reader = localcmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            datacenter = (string) reader["data_center"];
                            rack = (string) reader["rack"];
                        }
                    }

                    //make sure we actually found a datacenter or rack
                    if (datacenter == null || rack == null)
                        throw new CqlException(
                            "Could not detect datacenter or rack information from the node specified in the config section!");

                    //get the peers
                    var peerscmd = new CqlCommand(connection, "select rpc_address, data_center, rack from system.peers",
                                                  CqlConsistency.Any);
                    using (CqlDataReader reader = peerscmd.ExecuteReader())
                    {
                        //iterate over the peers
                        while (reader.Read())
                        {
                            var nodeDC = (string) reader["data_center"];
                            var nodeAddress = (IPAddress) reader["rpc_address"];
                            var nodeRack = (string) reader["rack"];

                            //filter based on scope
                            switch (config.DiscoveryScope)
                            {
                                case DiscoveryScope.DataCenter:
                                    if (!datacenter.Equals(nodeDC, StringComparison.InvariantCultureIgnoreCase))
                                        continue;
                                    break;

                                case DiscoveryScope.Rack:
                                    if (!datacenter.Equals(nodeDC, StringComparison.InvariantCultureIgnoreCase))
                                        continue;
                                    if (!rack.Equals(nodeRack, StringComparison.InvariantCultureIgnoreCase))
                                        continue;
                                    break;
                            }

                            //add the node if we did not find it yet
                            if (!found.Any(n => n.Address.Equals(nodeAddress)))
                            {
                                var newNode = new Node(nodeAddress, config);
                                found.Add(newNode);
                            }
                        }
                    }
                }
            }

            nodes.AddRange(found);
        }
    }
}