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
using CqlSharp.Network;
using CqlSharp.Network.Partition;
using CqlSharp.Protocol;
using System;
using System.Collections.Concurrent;
using System.Data;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp
{
    /// <summary>
    ///   A connection to a Cassandra cluster
    /// </summary>
    public class CqlConnection : IDbConnection
    {
        private static readonly ConcurrentDictionary<string, ClusterConfig> Configs;
        private static readonly ConcurrentDictionary<ClusterConfig, Cluster> Clusters;
        private Cluster _cluster;

        private Connection _connection;
        private int _state; //0=created; 1=opened; 2=disposed

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
            SetCluster(connectionString);
        }

        /// <summary>
        /// Sets the cluster.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        private void SetCluster(string connectionString)
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

            //set the connection provider to the cluster
            _cluster = cluster;
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

            //set the connection provider to the found cluster
            _cluster = cluster;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlConnection" /> class.
        /// </summary>
        /// <param name="cluster"> The cluster. </param>
        internal CqlConnection(Cluster cluster)
        {
            _cluster = cluster;
        }


        IDbTransaction IDbConnection.BeginTransaction(IsolationLevel il)
        {
            throw new NotSupportedException();
        }

        IDbTransaction IDbConnection.BeginTransaction()
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Changes the current database for an open Connection object.
        /// </summary>
        /// <param name="databaseName">The name of the database to use in place of the current database.</param>
        /// <exception cref="System.ObjectDisposedException">CqlConnection</exception>
        /// <exception cref="System.InvalidOperationException">Database property only supported with a ConnectionStrategy that offers exclusive connections</exception>
        public void ChangeDatabase(string databaseName)
        {
            if (_state == 0)
                throw new InvalidOperationException("Can set database only when the connection is open");

            if (_state == 2)
                throw new ObjectDisposedException("CqlConnection");

            if (!_cluster.ConnectionStrategy.ProvidesExclusiveConnections)
                throw new InvalidOperationException("Changing a database is only supported with a ConnectionStrategy that offers exclusive connections");

            _database = databaseName;
        }

        public void Close()
        {
            //TODO: create intermediate states
            Dispose(true);
        }

        public string ConnectionString
        {
            get { return _cluster.Config.ToString(); }
            set
            {
                if (_state != 0)
                    throw new InvalidOperationException("Can set connection string only when the connection is closed");

                SetCluster(value);
            }
        }

        /// <summary>
        /// Gets the time to wait while trying to establish a connection before terminating the attempt and generating an error.
        /// </summary>
        /// <returns>The time (in seconds) to wait for a connection to open. The default value is 15 seconds.</returns>
        /// <exception cref="System.NotSupportedException"></exception>
        int IDbConnection.ConnectionTimeout
        {
            get { throw new NotSupportedException(); }
        }

        /// <summary>
        /// Creates and returns a Command object associated with the connection.
        /// </summary>
        /// <returns>
        /// A Command object associated with the connection.
        /// </returns>
        public IDbCommand CreateCommand()
        {
            return new CqlCommand(this);
        }

        private string _database;

        /// <summary>
        /// Gets the name of the current database or the database to be used after a connection is opened.
        /// </summary>
        /// <returns>The name of the current database or the name of the database to be used once a connection is open. The default value is an empty string.</returns>
        /// <exception cref="System.InvalidOperationException">You must invoke Open or OpenAsync on a CqlConnection before other use.</exception>
        /// <exception cref="System.ObjectDisposedException">CqlConnection</exception>
        public string Database
        {
            get
            {
                if (_state == 2)
                    throw new ObjectDisposedException("CqlConnection");

                if (!_cluster.ConnectionStrategy.ProvidesExclusiveConnections)
                    throw new InvalidOperationException("Database property only supported with a ConnectionStrategy that offers exclusive connections");

                return _database;
            }
        }

        ConnectionState IDbConnection.State
        {
            get { return _state == 1 ? ConnectionState.Open : ConnectionState.Closed; }
        }


        /// <summary>
        ///   Gets or sets the throttle.
        /// </summary>
        /// <value> The throttle. </value>
        internal SemaphoreSlim Throttle
        {
            get
            {
                if (_state == 0)
                    throw new InvalidOperationException(
                        "You must invoke Open or OpenAsync on a CqlConnection before other use.");

                if (_state == 2)
                    throw new ObjectDisposedException("CqlConnection");

                return _cluster.Throttle;
            }
        }

        /// <summary>
        ///   Gets the config related to this connection.
        /// </summary>
        /// <value> The config </value>
        internal ClusterConfig Config
        {
            get { return _cluster.Config; }
        }

        /// <summary>
        ///   Gets the logger manager.
        /// </summary>
        /// <value> The logger manager. </value>
        internal LoggerManager LoggerManager
        {
            get { return _cluster.LoggerManager; }
        }



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
            int state = Interlocked.CompareExchange(ref _state, 1, 0);

            //make sure we are not opened yet
            if (state == 1)
                return;

            //make sure we are not disposed
            if (_state == 2)
                throw new ObjectDisposedException("CqlConnection");

            var logger = LoggerManager.GetLogger("CqlSharp.CqlConnection.Open");

            //make sure the cluster is open for connections
            await _cluster.OpenAsync(logger).ConfigureAwait(false);

            //get a connection
            _connection = _cluster.ConnectionStrategy.GetOrCreateConnection(ConnectionScope.Connection, PartitionKey.None);
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
        ///   Requests a connection to be used for a command. Will reuse connection level connection if available
        /// </summary>
        /// <returns> An open connection </returns>
        internal Connection GetConnection(PartitionKey partitionKey = default(PartitionKey))
        {
            if (_state == 0)
                throw new CqlException("You must invoke Open or OpenAsync on a CqlConnection before other use.");

            if (_state == 2)
                throw new ObjectDisposedException("CqlConnection");

            //reuse the connection if any available on connection level
            Connection connection = _connection ?? _cluster.ConnectionStrategy.GetOrCreateConnection(ConnectionScope.Command, partitionKey);

            if (connection == null)
                throw new CqlException("Unable to obtain a Cql network connection.");

            return connection;
        }

        /// <summary>
        /// Gets a value indicating whether the CqlConnection provides exclusive connections.
        /// </summary>
        /// <value>
        /// <c>true</c> if [provides exclusive connections]; otherwise, <c>false</c>.
        /// </value>
        internal bool ProvidesExclusiveConnections
        {
            get { return _cluster.ConnectionStrategy.ProvidesExclusiveConnections; }
        }

        /// <summary>
        ///   Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"> <c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources. </param>
        protected void Dispose(bool disposing)
        {
            if (disposing && (Interlocked.Exchange(ref _state, 2) != 2))
            {
                if (_connection != null)
                {
                    _cluster.ConnectionStrategy.ReturnConnection(_connection, ConnectionScope.Connection);
                    _connection = null;
                }
                _cluster = null;
            }
        }

        /// <summary>
        ///   Finalizes an instance of the <see cref="CqlConnection" /> class.
        /// </summary>
        ~CqlConnection()
        {
            Dispose(false);
        }

        internal ConcurrentDictionary<IPAddress, ResultFrame> GetPrepareResultsFor(string cql)
        {
            return _cluster.GetPrepareResultsFor(cql);
        }
    }
}