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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using CqlSharp.Logging;
using CqlSharp.Network.Partition;
using CqlSharp.Protocol;

namespace CqlSharp.Network
{
    /// <summary>
    /// Represents a Cassandra cluster
    /// </summary>
    internal class Cluster : IDisposable
    {
        private readonly CqlConnectionStringBuilder _config;
        private readonly LoggerManager _loggerManager;
        private readonly object _syncLock = new object();
        private IConnectionStrategy _connectionStrategy;
        private string _cqlVersion;
        private string _dataCenter;
        private bool _disposed;
        private Connection _maintenanceConnection;
        private string _name;
        private volatile Ring _nodes;
        private volatile Task _openTask;
        private string _rack;
        private string _release;
        private SemaphoreSlim _throttle;

        /// <summary>
        /// Initializes a new instance of the <see cref="Cluster" /> class.
        /// </summary>
        /// <param name="config"> The config. </param>
        internal Cluster(CqlConnectionStringBuilder config)
        {
            //store config
            _config = config;

            _loggerManager = new LoggerManager(_config.LoggerFactory, _config.LogLevel);
        }

        /// <summary>
        /// Gets the config
        /// </summary>
        /// <value> The config </value>
        public CqlConnectionStringBuilder Config
        {
            get { return _config; }
        }

        /// <summary>
        /// Gets the throttle to limit concurrent requests.
        /// </summary>
        /// <value> The throttle. </value>
        internal SemaphoreSlim Throttle
        {
            get
            {
                if(_disposed)
                    throw new ObjectDisposedException("Cluster");

                return _throttle;
            }
        }

        /// <summary>
        /// Gets the logger manager.
        /// </summary>
        /// <value> The logger manager. </value>
        public LoggerManager LoggerManager
        {
            get { return _loggerManager; }
        }


        /// <summary>
        /// Gets the connection strategy.
        /// </summary>
        /// <value> The connection strategy. </value>
        public IConnectionStrategy ConnectionStrategy
        {
            get
            {
                if(_disposed)
                    throw new ObjectDisposedException("Cluster");

                return _connectionStrategy;
            }
        }

        /// <summary>
        /// Gets the name of the cluster.
        /// </summary>
        /// <value> The name. </value>
        public string Name
        {
            get
            {
                if(_disposed)
                    throw new ObjectDisposedException("Cluster");

                return _name;
            }
        }

        /// <summary>
        /// Gets the rack to which the initial (seed) connection was made
        /// </summary>
        /// <value> The rack. </value>
        public string Rack
        {
            get
            {
                if(_disposed)
                    throw new ObjectDisposedException("Cluster");

                return _rack;
            }
        }

        /// <summary>
        /// Gets the data center to which the initial (seed) connection was made.
        /// </summary>
        /// <value> The data center. </value>
        public string DataCenter
        {
            get
            {
                if(_disposed)
                    throw new ObjectDisposedException("Cluster");

                return _dataCenter;
            }
        }

        /// <summary>
        /// Gets the cassandra version.
        /// </summary>
        /// <value> The cassandra version. </value>
        public string CassandraVersion
        {
            get
            {
                if(_disposed)
                    throw new ObjectDisposedException("Cluster");

                return _release;
            }
        }

        /// <summary>
        /// Gets the CQL version.
        /// </summary>
        /// <value> The CQL version. </value>
        public string CqlVersion
        {
            get
            {
                if(_disposed)
                    throw new ObjectDisposedException("Cluster");

                return _cqlVersion;
            }
        }

        /// <summary>
        /// Gets the prepare results for the given query
        /// </summary>
        internal ConcurrentDictionary<string, ResultFrame> PreparedQueryCache { get; private set; }

        #region IDisposable Members

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        public void Dispose()
        {
            if(!_disposed)
            {
                _disposed = true;

                Logger.Current.LogVerbose("Starting shutdown of {0}", this);
                try
                {
                    if(_throttle != null)
                        _throttle.Dispose();

                    if(_nodes != null)
                    {
                        foreach(var node in _nodes)
                            node.Dispose();
                    }

                    if(_maintenanceConnection != null)
                        _maintenanceConnection.Dispose();
                }
                catch(Exception ex)
                {
                    Logger.Current.LogWarning("Error occured while disposing cluster: {0}", ex);
                }
            }
        }

        #endregion

        /// <summary>
        /// Opens the cluster for queries.
        /// </summary>
        internal Task OpenAsync(Logger logger, CancellationToken token)
        {
            if(_disposed)
                throw new ObjectDisposedException("Cluster");

            if(_openTask == null || _openTask.IsFaulted || _openTask.IsCanceled)
            {
                lock(_syncLock)
                {
                    if(_openTask == null || _openTask.IsFaulted || _openTask.IsCanceled)
                    {
                        //set the openTask
                        _openTask = OpenAsyncInternal(logger, token);
                    }
                }
            }

            return _openTask;
        }

        /// <summary>
        /// Opens the cluster for queries. Contains actual implementation and will be called only once per cluster
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="CqlException">Cannot construct ring from provided seeds!</exception>
        private async Task OpenAsyncInternal(Logger logger, CancellationToken token)
        {
            logger.LogInfo("Opening Cluster with parameters: {0}", _config.ToString());

            //initialize the ring
            _nodes = new Ring();

            //try to connect to the seeds in turn
            foreach(IPAddress seedAddress in _config.ServerAddresses)
            {
                try
                {
                    var seed = new Node(seedAddress, this);
                    await GetClusterInfoAsync(seed, logger, token).ConfigureAwait(false);
                }
                catch(TaskCanceledException)
                {
                    logger.LogWarning("Opening connection to cluster was cancelled");
                    throw;
                }
                catch(ProtocolException pex)
                {
                    //node is not available, or starting up, try next, otherwise throw error
                    if(pex.Code != ErrorCode.Overloaded && pex.Code != ErrorCode.IsBootstrapping)
                        throw;
                }
                catch(SocketException ex)
                {
                    //seed not reachable, try next
                    logger.LogWarning("Could not open TCP connection to seed {0}: {1}", seedAddress, ex);
                }
                catch(IOException ex)
                {
                    //seed not reachable, try next
                    logger.LogWarning("Could not discover nodes via seed {0}: {1}", seedAddress, ex);
                }
            }

            //check if not disposed while opening
            if(_disposed)
            {
                foreach(var node in _nodes)
                    node.Dispose();
                throw new ObjectDisposedException("Cluster", "Cluster was disposed while opening");
            }

            //check if we found any nodes
            if(_nodes.Count == 0)
            {
                var ex = new CqlException("Unable to connect to the cluster as none of the provided seeds is reachable.");
                logger.LogCritical("Unable to setup Cluster based on given configuration: {0}", ex);
                throw ex;
            }

            //setup cluster connection strategy
            switch(_config.ConnectionStrategy)
            {
                case CqlSharp.ConnectionStrategy.Balanced:
                    _connectionStrategy = new BalancedConnectionStrategy(_nodes, _config);
                    break;
                case CqlSharp.ConnectionStrategy.Random:
                    _connectionStrategy = new RandomConnectionStrategy(_nodes, _config);
                    break;
                case CqlSharp.ConnectionStrategy.Exclusive:
                    _connectionStrategy = new ExclusiveConnectionStrategy(_nodes, _config);
                    break;
                case CqlSharp.ConnectionStrategy.PartitionAware:
                    _connectionStrategy = new PartitionAwareConnectionStrategy(_nodes, _config);
                    if(_config.DiscoveryScope != DiscoveryScope.Cluster ||
                       _config.DiscoveryScope != DiscoveryScope.DataCenter)
                    {
                        logger.LogWarning(
                            "PartitionAware connection strategy performs best if DiscoveryScope is set to cluster or datacenter");
                    }
                    break;
            }

            //setup throttle
            int concurrent = _config.MaxConcurrentQueries <= 0
                ? _nodes.Count*_config.MaxConnectionsPerNode*256
                : _config.MaxConcurrentQueries;

            logger.LogInfo("Cluster is configured to allow {0} parallel queries", concurrent);

            _throttle = new SemaphoreSlim(concurrent, concurrent);

            //setup prepared query cache
            PreparedQueryCache = new ConcurrentDictionary<string, ResultFrame>();

            //setup maintenance connection
            SetupMaintenanceConnection(logger);
        }

        /// <summary>
        /// Setups the maintenance channel.
        /// </summary>
        private async void SetupMaintenanceConnection(Logger logger)
        {
            //skip if disposed
            if(_disposed)
                return;

            try
            {
                if(_maintenanceConnection == null || !_maintenanceConnection.IsConnected)
                {
                    //setup maintenance connection
                    logger.LogVerbose("Creating new maintenance connection");

                    //get or create a connection
                    Connection connection;
                    using(logger.ThreadBinding())
                    {
                        connection = _connectionStrategy.GetOrCreateConnection(ConnectionScope.Infrastructure, null);
                    }

                    //check if we really got a connection
                    if(connection == null)
                        throw new CqlException("Can not obtain connection for maintenance channel");

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
            catch(Exception ex)
            {
                logger.LogWarning("Failed to setup maintenance connection: {0}", ex);
                //temporary disconnect or registration failed, reset maintenance connection
                _maintenanceConnection = null;
            }

            //don't retry if disposed
            if(_disposed)
                return;

            //wait a moment, try again
            logger.LogVerbose("Waiting 2secs before retrying setup maintenance connection");
            await Task.Delay(2000).ConfigureAwait(false);

            SetupMaintenanceConnection(logger);
        }

        /// <summary>
        /// Gets all nodes that make up the cluster
        /// </summary>
        /// <param name="seed"> The reference. </param>
        /// <param name="logger"> logger used to log progress </param>
        /// <param name="token"> The token. </param>
        /// <returns> </returns>
        /// <exception cref="CqlException">
        /// Could not detect datacenter or rack information from the reference specified in the
        /// config section!
        /// </exception>
        private async Task GetClusterInfoAsync(Node seed, Logger logger, CancellationToken token)
        {
            Connection c;
            using(logger.ThreadBinding())
            {
                //get a connection
                if(seed != null && seed.IsUp)
                    c = seed.GetOrCreateConnection(null);
                else if(_maintenanceConnection != null && _maintenanceConnection.IsConnected)
                {
                    c = _maintenanceConnection;
                    seed = c.Node;
                }
                else
                {
                    c = _connectionStrategy.GetOrCreateConnection(ConnectionScope.Infrastructure, null);
                    seed = c.Node;
                }
            }

            //get local information
            string partitioner;
            using(
                var result =
                    await
                        ExecQuery(c,
                                  "select cluster_name, cql_version, release_version, partitioner, data_center, rack, tokens from system.local",
                                  logger, token).ConfigureAwait(false))
            {
                if(!await result.ReadAsync().ConfigureAwait(false))
                    throw new CqlException("Could not detect the cluster partitioner");
                _name = result.GetString(0);
                _cqlVersion = result.GetString(1);
                _release = result.GetString(2);
                partitioner = result.GetString(3);
                _dataCenter = seed.DataCenter = result.GetString(4);
                _rack = seed.Rack = result.GetString(5);
                seed.Tokens = result.GetSet<string>(6) ?? new HashSet<string>();
            }

            logger.LogInfo(
                "Reconfigured cluster {0}: based on Cassandra Release {1}, supporting CqlVersion {2}, using partitioner '{3}'",
                _name, _release, _cqlVersion, partitioner);

            //create list of nodes that make up the cluster, and add the seed
            var found = new List<Node> {seed};

            //get the peers
            using(
                var result =
                    await
                        ExecQuery(c, "select peer, rpc_address, data_center, rack, tokens from system.peers", logger,
                                  token).
                            ConfigureAwait(false))
            {
                //iterate over the peers
                while(await result.ReadAsync().ConfigureAwait(false))
                {
                    var newNode = GetNodeFromDataReader(result, logger);

                    //add it if it is in scope
                    if(InDiscoveryScope(seed, newNode, _config.DiscoveryScope))
                        found.Add(newNode);
                }
            }

            //set the new Ring of nodes
            _nodes.Update(found, partitioner, logger);

            //check if all tokens are received
            if(_nodes.Any(n => n.Tokens.Count == 0))
            {
                //wait and retry the fetch later...
                var retry = Task.Run(async () =>
                {
                    try
                    {
                        logger.LogInfo("Cluster info incomplete scheduling new retrieval in 1 minute");
                        await Task.Delay(TimeSpan.FromMinutes(1));
                        await GetClusterInfoAsync(null, logger, CancellationToken.None);
                    }
                    catch(Exception ex)
                    {
                        logger.LogCritical("Critical error occured while updating cluster info: {0}", ex);
                    }
                });
            }
        }

        private Node GetNodeFromDataReader(CqlDataReader reader, Logger logger)
        {
            //get address of new node, and fallback to listen_address when address is set to any
            var address = reader["rpc_address"] as IPAddress;
            if(address == null || address.Equals(IPAddress.Any))
                address = reader["peer"] as IPAddress;

            var dc = reader["data_center"] as string;
            var rack = reader["rack"] as string;

            //check if we have an address, otherwise ignore
            if(address == null || dc == null || rack == null)
            {
                logger.LogError("Incomplete node information retrieved for a node: address={0}, dc={1}, rack={2}",
                                address != null ? address.ToString() : "(address not found!)",
                                dc ?? "(datacenter not found)",
                                rack ?? "(rack not found)");

                return null;
            }

            var tokens = (reader["tokens"] as ISet<string>) ?? new HashSet<string>();

            //create a new node
            return new Node(address, this)
            {
                DataCenter = dc,
                Rack = rack,
                Tokens = tokens
            };
        }

        /// <summary>
        /// Executes a query.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="cql"> The CQL. </param>
        /// <param name="logger"> The logger. </param>
        /// <param name="token"> The token. </param>
        /// <returns> A CqlDataReader that can be used to access the query results </returns>
        private async Task<CqlDataReader> ExecQuery(Connection connection, string cql, Logger logger,
                                                    CancellationToken token)
        {
            //cancel if requested
            token.ThrowIfCancellationRequested();

            logger.LogVerbose("Excuting query {0} on {1}", cql, connection);

            var query = new QueryFrame(cql, CqlConsistency.One, null);
            var result =
                (ResultFrame)await connection.SendRequestAsync(query, logger, 1, false, token).ConfigureAwait(false);
            var reader = new CqlDataReader(null, result, null);

            logger.LogVerbose("Query {0} returned {1} results", cql, reader.Count);

            return reader;
        }

        /// <summary>
        /// Checks wether a node is in the discovery scope.
        /// </summary>
        /// <param name="reference"> The reference node that is known to be in scope </param>
        /// <param name="target"> The target node of which is checked wether it is in scope </param>
        /// <param name="discoveryScope"> The discovery scope. </param>
        /// <returns> </returns>
        private bool InDiscoveryScope(Node reference, Node target, DiscoveryScope discoveryScope)
        {
            //filter based on scope
            switch(discoveryScope)
            {
                case DiscoveryScope.None:
                    //add if item is in configured list
                    return _config.ServerAddresses.Contains(target.Address);

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
            if(_disposed)
                return;

            var logger = LoggerManager.GetLogger("CqlSharp.Cluster.Changes");

            try
            {
                if(args.Change.Equals(ClusterChange.New) || args.Change.Equals(ClusterChange.Removed))
                {
                    logger.LogVerbose("Cluster changed: {0} is {1}", args.Node, args.Change);

                    //get the connection from which we received the event
                    var connection = (Connection)source;
                    var node = connection.Node;

                    //refetch the cluster configuration
                    await GetClusterInfoAsync(node, logger, CancellationToken.None);
                }
                else if(args.Change.Equals(ClusterChange.Up))
                {
                    Node upNode = _nodes.FirstOrDefault(node => args.Node.Equals(node.Address));

                    if(upNode != null)
                    {
                        using(logger.ThreadBinding())
                        {
                            upNode.Reactivate();
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                logger.LogError("Exception occured while handling cluster change {0} - {1}: {2}", args.Node, args.Change,
                                ex);
            }
        }

        /// <summary>
        /// Returns a <see cref="System.String" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format("Cluster (\"{0}\")", _name);
        }
    }
}