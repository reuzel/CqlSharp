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
using CqlSharp.Logging;
using CqlSharp.Network.Partition;
using CqlSharp.Protocol;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp.Network
{
    /// <summary>
    ///   Represents a Cassandra cluster
    /// </summary>
    internal class Cluster
    {
        private readonly ClusterConfig _config;
        private IConnectionStrategy _connectionSelector;
        private Semaphore _throttle;
        private volatile Task _openTask;
        private readonly object _syncLock = new object();
        private volatile Ring _nodes;
        private Connection _maintenanceConnection;
        private ConcurrentDictionary<string, ConcurrentDictionary<IPAddress, ResultFrame>> _prepareResultCache;
        private LoggerManager _loggerManager;

        /// <summary>
        ///   Initializes a new instance of the <see cref="Cluster" /> class.
        /// </summary>
        /// <param name="config"> The config. </param>
        public Cluster(ClusterConfig config)
        {
            //store config
            _config = config;

            _loggerManager = new LoggerManager(_config.LoggerFactory, _config.LogLevel);
        }

        /// <summary>
        /// Gets the config
        /// </summary>
        /// <value>
        /// The config
        /// </value>
        public ClusterConfig Config { get { return _config; } }

        /// <summary>
        /// Opens the cluster for queries.
        /// </summary>
        public Task OpenAsync(Logger logger)
        {
            if (_openTask == null || _openTask.IsFaulted)
            {
                lock (_syncLock)
                {
                    if (_openTask == null || _openTask.IsFaulted)
                    {
                        //set the openTask
                        _openTask = OpenAsyncInternal(logger);
                    }
                }
            }

            return _openTask;
        }

        /// <summary>
        /// Opens the cluster for queries. Contains actual implementation and will be called only once per cluster
        /// </summary>
        /// <returns></returns>
        /// <exception cref="CqlException">Cannot construct ring from provided seeds!</exception>
        private async Task OpenAsyncInternal(Logger logger)
        {
            logger.LogInfo("Opening Cluster with parameters: {0}", _config.ToString());

            //try to connect to the seeds in turn
            foreach (IPAddress seedAddress in _config.NodeAddresses)
            {
                try
                {
                    var seed = new Node(seedAddress, this);
                    _nodes = await DiscoverNodesAsync(seed, logger).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    //seed not reachable, try next
                    logger.LogWarning("Could not discover nodes via seed {0}: {1}", seedAddress, ex);
                }
            }

            if (_nodes == null)
            {
                var ex = new CqlException("Cannot construct ring from provided seeds!");
                logger.LogCritical("Unable to setup Cluster based on given configuration: {0}", ex);
                throw ex;
            }

            logger.LogInfo("Nodes detected: " + string.Join(", ", _nodes.Select(n => n.Address)));

            //setup cluster connection strategy
            switch (_config.ConnectionStrategy)
            {
                case ConnectionStrategy.Balanced:
                    _connectionSelector = new BalancedConnectionStrategy(_nodes, _config);
                    break;
                case ConnectionStrategy.Random:
                    _connectionSelector = new RandomConnectionStrategy(_nodes, _config);
                    break;
                case ConnectionStrategy.Exclusive:
                    _connectionSelector = new ExclusiveConnectionStrategy(_nodes, _config);
                    break;
                case ConnectionStrategy.PartitionAware:
                    _connectionSelector = new PartitionAwareConnectionStrategy(_nodes, _config);
                    if (_config.DiscoveryScope != DiscoveryScope.Cluster || _config.DiscoveryScope != DiscoveryScope.DataCenter) logger.LogWarning("PartitionAware connection strategy performs best if DiscoveryScope is set to cluster or datacenter");
                    break;
            }

            //setup throttle
            int concurrent = _config.MaxConcurrentQueries <= 0
                                 ? _nodes.Count * _config.MaxConnectionsPerNode * 256
                                 : _config.MaxConcurrentQueries;

            logger.LogInfo("Cluster is configured to allow {0} parallel queries", concurrent);

            _throttle = new Semaphore(concurrent, concurrent);

            //setup prepared query cache
            _prepareResultCache = new ConcurrentDictionary<string, ConcurrentDictionary<IPAddress, ResultFrame>>();

            //setup maintenance connection
            SetupMaintenanceConnection(logger);
        }

        /// <summary>
        /// Setups the maintenance channel.
        /// </summary>
        private async void SetupMaintenanceConnection(Logger logger)
        {
            try
            {
                if (_maintenanceConnection == null || !_maintenanceConnection.IsConnected)
                {
                    //setup maintenance connection
                    logger.LogVerbose("Creating new maintenance connection");

                    //pick a random node from the list
                    var strategy = new RandomConnectionStrategy(_nodes, _config);

                    //get or create a connection
                    var connection = strategy.GetOrCreateConnection(null);

                    //setup event handlers
                    connection.OnConnectionChange += (src, ev) => SetupMaintenanceConnection(logger);
                    connection.OnClusterChange += OnClusterChange;

                    //store the new connection
                    _maintenanceConnection = connection;

                    //register for events
                    await connection.RegisterForClusterChangesAsync(logger).ConfigureAwait(false);

                    logger.LogInfo("Registered for cluster changes using {0}", connection);
                }

                //all seems right, we're done
                return;
            }
            catch (Exception ex)
            {
                logger.LogWarning("Failed to setup maintenance connection: {0}", ex);
                //temporary disconnect or registration failed, reset maintenance connection
                _maintenanceConnection = null;
            }

            //wait a moment, try again
            logger.LogVerbose("Waiting 2secs before retrying setup maintenance connection");
            await Task.Delay(2000).ConfigureAwait(false);

            SetupMaintenanceConnection(logger);
        }

        /// <summary>
        ///   Gets the throttle to limit concurrent requests.
        /// </summary>
        /// <value> The throttle. </value>
        public Semaphore Throttle
        {
            get { return _throttle; }
        }

        public LoggerManager LoggerManager
        {
            get { return _loggerManager; }
        }


        /// <summary>
        /// Gets the prepare results for the given query
        /// </summary>
        /// <param name="cql">The CQL query prepared</param>
        /// <returns></returns>
        internal ConcurrentDictionary<IPAddress, ResultFrame> GetPrepareResultsFor(string cql)
        {
            return _prepareResultCache.GetOrAdd(cql, s => new ConcurrentDictionary<IPAddress, ResultFrame>());
        }


        /// <summary>
        ///   Gets a connection to a reference in the cluster
        /// </summary>
        /// <param name="partitionKey"> </param>
        /// <returns> </returns>
        public Connection GetOrCreateConnection(PartitionKey partitionKey)
        {
            return _connectionSelector.GetOrCreateConnection(partitionKey);
        }

        /// <summary>
        ///   Returns the connection.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        public void ReturnConnection(Connection connection)
        {
            _connectionSelector.ReturnConnection(connection);
        }


        /// <summary>
        /// Gets all nodes that make up the cluster
        /// </summary>
        /// <param name="seed">The reference.</param>
        /// <param name="logger">logger used to log progress</param>
        /// <returns></returns>
        /// <exception cref="CqlException">Could not detect datacenter or rack information from the reference specified in the config section!</exception>
        private async Task<Ring> DiscoverNodesAsync(Node seed, Logger logger)
        {
            Connection c;
            using (logger.ThreadBinding())
            {
                //get a connection
                c = seed.GetOrCreateConnection(null);
            }

            //get partitioner
            string partitioner;
            using (var result = await ExecQuery(c, "select partitioner from system.local", logger).ConfigureAwait(false))
            {
                if (!await result.ReadAsync().ConfigureAwait(false))
                    throw new CqlException("Could not detect the cluster partitioner");
                partitioner = (string)result[0];
            }

            logger.LogInfo("Partitioner in use: {0}", partitioner);

            //get the "local" data center, rack and token
            using (var result = await ExecQuery(c, "select data_center, rack, tokens from system.local", logger).ConfigureAwait(false))
            {
                if (await result.ReadAsync().ConfigureAwait(false))
                {
                    seed.DataCenter = (string)result["data_center"];
                    seed.Rack = (string)result["rack"];
                    seed.Tokens = (ISet<string>)result["tokens"];

                    logger.LogVerbose("Seed info - Address:{0} DataCenter:{1} Rack:{2}", seed.Address, seed.DataCenter, seed.Rack);
                }
                else
                {
                    //strange, no local info found?!
                    throw new CqlException("Could not detect datacenter or rack information from the reference specified in the config section!");
                }
            }

            //create list of nodes that make up the cluster, and add the seed
            var found = new List<Node> { seed };

            //get the peers
            using (var result = await ExecQuery(c, "select rpc_address, data_center, rack, tokens from system.peers", logger).ConfigureAwait(false))
            {
                //iterate over the peers
                while (await result.ReadAsync().ConfigureAwait(false))
                {
                    //create a new node
                    var newNode = new Node((IPAddress)result["rpc_address"], this)
                                        {
                                            DataCenter = (string)result["data_center"],
                                            Rack = (string)result["rack"],
                                            Tokens = (ISet<string>)result["tokens"]
                                        };

                    //add it if it is in scope
                    if (InDiscoveryScope(seed, newNode, _config.DiscoveryScope))
                        found.Add(newNode);
                }
            }

            //return a new Ring of nodes
            return new Ring(found, partitioner);
        }

        /// <summary>
        /// Executes a query.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="cql">The CQL.</param>
        /// <param name="logger">The logger.</param>
        /// <returns>
        /// A CqlDataReader that can be used to access the query results
        /// </returns>
        private async Task<CqlDataReader> ExecQuery(Connection connection, string cql, Logger logger)
        {
            logger.LogVerbose("Excuting query {0} on {1}", cql, connection);

            var query = new QueryFrame(cql, CqlConsistency.One);
            var result = (ResultFrame)await connection.SendRequestAsync(query, logger).ConfigureAwait(false);
            var reader = new CqlDataReader(result);

            logger.LogVerbose("Query {0} returned {1} results", cql, reader.Count);

            return reader;
        }

        /// <summary>
        /// Checks wether a node is in the discovery scope.
        /// </summary>
        /// <param name="reference">The reference node that is known to be in scope</param>
        /// <param name="target">The target node of which is checked wether it is in scope</param>
        /// <param name="discoveryScope">The discovery scope.</param>
        /// <returns></returns>
        private bool InDiscoveryScope(Node reference, Node target, DiscoveryScope discoveryScope)
        {
            //filter based on scope
            switch (discoveryScope)
            {
                case DiscoveryScope.None:
                    //add if item is in configured list
                    return _config.NodeAddresses.Contains(target.Address);

                case DiscoveryScope.DataCenter:
                    //add if in the same datacenter
                    return target.DataCenter.Equals(reference.DataCenter);

                case DiscoveryScope.Rack:
                    //add if reference matches datacenter and rack
                    return target.DataCenter.Equals(reference.DataCenter) && target.Rack.Equals(reference.Rack);

                case DiscoveryScope.Cluster:
                    //add all
                    return true;
            }

            return false;
        }

        private async void OnClusterChange(object source, ClusterChangedEvent args)
        {
            var logger = LoggerManager.GetLogger("CqlSharp.Cluster.Changes");

            if (args.Change.Equals(ClusterChange.New))
            {
                //get the connection from which we received the event
                var connection = (Connection)source;

                //get the new peer
                using (var result = await ExecQuery(connection, "select rpc_address, data_center, rack, tokens from system.peers where peer = '" + args.Node + "'", logger).ConfigureAwait(false))
                {
                    if (await result.ReadAsync().ConfigureAwait(false))
                    {
                        var newNode = new Node((IPAddress)result["rpc_address"], this)
                                            {
                                                DataCenter = (string)result["data_center"],
                                                Rack = (string)result["rack"],
                                                Tokens = (ISet<string>)result["tokens"]
                                            };


                        if (InDiscoveryScope(_nodes.First(), newNode, _config.DiscoveryScope))
                        {
                            logger.LogInfo("{0} added to the cluster", newNode);
                            _nodes.Add(newNode);
                        }
                        else
                        {
                            logger.LogVerbose("new {0} is ignored as it does not fit in the discovery scope", newNode);
                        }
                    }
                }
            }
            else if (args.Change.Equals(ClusterChange.Removed))
            {
                Node removedNode = _nodes.FirstOrDefault(node => args.Node.Equals(node.Address));
                if (removedNode != null)
                {
                    _nodes.Remove(removedNode);
                    logger.LogInfo("{0} was removed from the cluster", removedNode);
                }
                else
                {
                    logger.LogVerbose("Node with address {0} was removed but not used within the current configuration", args.Node);
                }
            }
            else if (args.Change.Equals(ClusterChange.Up))
            {
                Node upNode = _nodes.FirstOrDefault(node => args.Node.Equals(node.Address));

                if (upNode != null)
                {
                    using (logger.ThreadBinding())
                    {
                        upNode.Reactivate();
                    }
                }
            }
        }
    }
}