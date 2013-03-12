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
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using CqlSharp.Config;
using CqlSharp.Network;

namespace CqlSharp
{
    /// <summary>
    ///   A connection to a Cassandra cluster
    /// </summary>
    public class CqlConnection : IDisposable
    {
        private static readonly ConcurrentDictionary<string, ClusterConfig> Configs;
        private static readonly ConcurrentDictionary<ClusterConfig, Cluster> Clusters;

        private Connection _connection;
        private int _disposed;
        private IConnectionProvider _provider;

        static CqlConnection()
        {
            Clusters = new ConcurrentDictionary<ClusterConfig, Cluster>();
            Configs = new ConcurrentDictionary<string, ClusterConfig>();
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlConnection" /> class.
        /// </summary>
        /// <param name="connectionString"> The connection string. </param>
        public CqlConnection(string connectionString)
        {
            //get the cluster config, or add one if none exists
            ClusterConfig config = Configs.GetOrAdd(connectionString, connString =>
                                                                          {
                                                                              //create new config
                                                                              var cc = new ClusterConfig(connString);

                                                                              //get if a similar already exists, or add it otherwise
                                                                              return Configs.GetOrAdd(cc.ToString(), cc);
                                                                          });

            //fetch the cluster, or create one
            Cluster cluster = Clusters.GetOrAdd(config, conf => new Cluster(conf));

            //set the throttle to the one from the active cluster
            Throttle = cluster.Throttle;

            //set the connection provider to the cluster
            _provider = cluster;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlConnection" /> class.
        /// </summary>
        /// <param name="config"> The config. </param>
        public CqlConnection(ClusterConfig config)
        {
            //add the config to the list, or get an existing instance with the same parameters
            ClusterConfig c = Configs.GetOrAdd(config.ToString(), config);

            //get the cluster based on the instance
            Cluster cluster = Clusters.GetOrAdd(c, conf => new Cluster(conf));

            //set the throttle of this connection to the one from the cluster
            Throttle = cluster.Throttle;

            //set the connection provider to the found cluster
            _provider = cluster;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlConnection" /> class.
        /// </summary>
        /// <param name="node"> The node. </param>
        internal CqlConnection(Node node)
        {
            _provider = node;
            Throttle = new SemaphoreSlim(128);
        }

        /// <summary>
        ///   Gets or sets the throttle.
        /// </summary>
        /// <value> The throttle. </value>
        public SemaphoreSlim Throttle { get; set; }

        #region IDisposable Members

        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        /// <summary>
        ///   Opens the connection.
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="System.ObjectDisposedException">CqlConnection</exception>
        public async Task OpenAsync()
        {
            if (_disposed == 1)
                throw new ObjectDisposedException("CqlConnection");

            _connection = await _provider.GetOrCreateConnectionAsync();

            if (_connection == null)
                throw new CqlException("Unable to obtain a Cql network connection.");
        }

        /// <summary>
        ///   Opens the connection.
        /// </summary>
        /// <remark>This method is a convenience wrapper around OpenAsync()</remark>
        /// <exception cref="System.ObjectDisposedException">CqlConnection</exception>
        public void Open()
        {
            try
            {
                OpenAsync().Wait();
            }
            catch (AggregateException aex)
            {
                throw aex.InnerException;
            }
        }

        /// <summary>
        ///   Gets the underlying connection. Will reopen this CqlConnection, if the underlying connection has failed,
        /// </summary>
        /// <returns> An open connection </returns>
        internal async Task<Connection> GetConnectionAsync(bool newConnection = false)
        {
            if (_disposed == 1)
                throw new ObjectDisposedException("CqlConnection");

            //if new connection requested, get a new one from the provided
            if (newConnection)
            {
                Connection connection = await _provider.GetOrCreateConnectionAsync();

                if (connection == null)
                    throw new CqlException("Unable to obtain a Cql network connection.");

                return connection;
            }

            //reuse the reserved connection if it still connected
            if (_connection != null && _connection.IsConnected)
                return _connection;

            //no connection, re-open
            await OpenAsync();

            //return connection (if any can be made)
            return _connection;
        }

        /// <summary>
        ///   Returns the connection. Used when a connection is no longer needed by a command, or when this instance is disposed.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        internal void ReturnConnection(Connection connection)
        {
            if (connection != null)
            {
                _provider.ReturnConnection(connection);
            }
        }

        /// <summary>
        ///   Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"> <c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources. </param>
        protected void Dispose(bool disposing)
        {
            if (disposing && (Interlocked.CompareExchange(ref _disposed, 1, 0) == 0))
            {
                ReturnConnection(_connection);
                _connection = null;
                _provider = null;
            }
        }

        /// <summary>
        ///   Finalizes an instance of the <see cref="CqlConnection" /> class.
        /// </summary>
        ~CqlConnection()
        {
            Dispose(false);
        }
    }
}