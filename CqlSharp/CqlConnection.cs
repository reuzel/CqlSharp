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
        private static readonly ConcurrentDictionary<string, Cluster> Clusters;

        private Cluster _cluster;
        private Connection _connection;
        private string _database;
        private bool _disposed;
        private volatile Task _openTask;
        private CancellationTokenSource _openCancellationTokenSource;

        static CqlConnection()
        {
            Clusters = new ConcurrentDictionary<string, Cluster>();
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlConnection" /> class.
        /// </summary>
        /// <param name="connectionString"> The connection string. </param>
        public CqlConnection(string connectionString)
        {
            ConnectionString = connectionString;
            ConnectionTimeout = 0;
            _database = string.Empty;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlConnection" /> class.
        /// </summary>
        /// <param name="config"> The config. </param>
        public CqlConnection(ClusterConfig config)
            : this(config.ToString())
        {
        }

        /// <summary>
        ///   Gets or sets the throttle.
        /// </summary>
        /// <value> The throttle. </value>
        internal SemaphoreSlim Throttle
        {
            get
            {
                return Cluster.Throttle;
            }
        }

        /// <summary>
        ///   Gets the config related to this connection.
        /// </summary>
        /// <value> The config </value>
        internal ClusterConfig Config
        {
            get { return Cluster.Config; }
        }

        /// <summary>
        ///   Gets the logger manager.
        /// </summary>
        /// <value> The logger manager. </value>
        internal LoggerManager LoggerManager
        {
            get { return Cluster.LoggerManager; }
        }

        /// <summary>
        ///   Gets a value indicating whether the CqlConnection provides exclusive connections.
        /// </summary>
        /// <value> <c>true</c> if [provides exclusive connections]; otherwise, <c>false</c> . </value>
        internal bool ProvidesExclusiveConnections
        {
            get { return Cluster.ConnectionStrategy.ProvidesExclusiveConnections; }
        }

        #region IDbConnection Members

        IDbTransaction IDbConnection.BeginTransaction(IsolationLevel il)
        {
            throw new NotSupportedException();
        }

        IDbTransaction IDbConnection.BeginTransaction()
        {
            throw new NotSupportedException();
        }

        /// <summary>
        ///   Changes the current database for an open Connection object.
        /// </summary>
        /// <param name="databaseName"> The name of the database to use in place of the current database. </param>
        public void ChangeDatabase(string databaseName)
        {
            if (State != ConnectionState.Open)
                throw new InvalidOperationException("CqlConnection must be open before Database can be changed.");

            if (!Cluster.ConnectionStrategy.ProvidesExclusiveConnections)
                throw new InvalidOperationException(
                    "Changing a database is only supported with a ConnectionStrategy that offers exclusive connections");

            _database = databaseName;
        }

        /// <summary>
        ///   Gets or sets the string used to open a database.
        /// </summary>
        /// <returns> A string containing connection settings. </returns>
        public string ConnectionString { get; set; }

        /// <summary>
        ///   Gets or sets the time to wait while trying to establish a connection before terminating the attempt and generating an error.
        /// </summary>
        /// <returns> The time (in seconds) to wait for a connection to open. The default value is 15 seconds. </returns>
        public int ConnectionTimeout { get; set; }

        /// <summary>
        ///   Creates and returns a Command object associated with the connection.
        /// </summary>
        /// <returns> A Command object associated with the connection. </returns>
        public IDbCommand CreateCommand()
        {
            return new CqlCommand(this);
        }

        /// <summary>
        ///   Gets the name of the current database or the database to be used after a connection is opened.
        /// </summary>
        /// <returns> The name of the current database or the name of the database to be used once a connection is open. The default value is an empty string. </returns>
        /// <exception cref="System.InvalidOperationException">You must invoke Open or OpenAsync on a CqlConnection before other use.</exception>
        /// <exception cref="System.ObjectDisposedException">CqlConnection</exception>
        public string Database
        {
            get
            {
                if (_disposed)
                    throw new ObjectDisposedException("CqlConnection");

                return _database;
            }
        }


        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        ///   Opens a database connection with the settings specified by the ConnectionString property of the provider-specific Connection object.
        /// </summary>
        public void Open()
        {
            try
            {
                _openCancellationTokenSource = ConnectionTimeout > 0
                                       ? new CancellationTokenSource(TimeSpan.FromSeconds(ConnectionTimeout))
                                       : new CancellationTokenSource();

                OpenAsync(_openCancellationTokenSource.Token).Wait();
            }
            catch (AggregateException aex)
            {
                throw aex.InnerException;
            }
            finally
            {
                _openCancellationTokenSource = null;
            }
        }

        /// <summary>
        /// Cancels any ongoing open operation.
        /// </summary>
        public void CancelOpen()
        {
            if (_openCancellationTokenSource != null)
                _openCancellationTokenSource.Cancel();
        }

        /// <summary>
        ///   Closes the connection to the database.
        /// </summary>
        public void Close()
        {


            if (State != ConnectionState.Closed)
            {
                var logger = LoggerManager.GetLogger("CqlSharp.CqlConnection.Close");

                try
                {
                    //wait until open is finished (may return immediatly)
                    _openTask.Wait();
                }
                catch (Exception ex)
                {
                    logger.LogVerbose("Closing connection that was not opened correctly: ", ex);
                }

                //return connection if any
                if (_connection != null)
                {
                    Cluster.ConnectionStrategy.ReturnConnection(_connection, ConnectionScope.Connection);
                    _connection = null;
                }

                //clear cluster
                Cluster = null;

                //clear open task, such that open can be run again
                _openTask = null;
            }
        }

        /// <summary>
        ///   Gets the current state of the connection.
        /// </summary>
        /// <returns> One of the <see cref="T:System.Data.ConnectionState" /> values. </returns>
        public ConnectionState State
        {
            get
            {
                if (_disposed)
                    throw new ObjectDisposedException("CqlConnection");

                if (_openTask == null || _openTask.IsCanceled)
                    return ConnectionState.Closed;

                if (_openTask.IsFaulted || (_connection != null && !_connection.IsConnected))
                    return ConnectionState.Broken;

                if (!_openTask.IsCompleted)
                    return ConnectionState.Connecting;

                return ConnectionState.Open;
            }
        }

        /// <summary>
        /// Gets or sets the cluster.
        /// </summary>
        /// <value>
        /// The cluster.
        /// </value>
        /// <exception cref="System.InvalidOperationException">CqlConnection must be open before further use.</exception>
        private Cluster Cluster
        {
            get
            {
                if (State != ConnectionState.Open)
                    throw new InvalidOperationException("CqlConnection must be open before further use.");

                return _cluster;
            }

            set
            {
                _cluster = value;
            }
        }

        #endregion

        /// <summary>
        ///   Opens the connection.
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="System.ObjectDisposedException">CqlConnection</exception>
        public Task OpenAsync()
        {
            return OpenAsync(CancellationToken.None);
        }

        /// <summary>
        ///   Opens connection async.
        /// </summary>
        /// <param name="token"> The token. </param>
        /// <returns> </returns>
        private Task OpenAsync(CancellationToken token)
        {
            if (State != ConnectionState.Closed)
                throw new InvalidOperationException("Connection must be closed before it is opened");

            _openTask = OpenAsyncInternal(token);
            return _openTask;
        }

        /// <summary>
        ///   Opens the connection.
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="System.ObjectDisposedException">CqlConnection</exception>
        private async Task OpenAsyncInternal(CancellationToken token)
        {

            //get or add cluster based on connection string
            Cluster cluster = Clusters.GetOrAdd(ConnectionString, connString =>
                                                                      {
                                                                          //connection string unknown, create a new config 
                                                                          var cc = new ClusterConfig(connString);

                                                                          //get normalized connection string
                                                                          string normalizedConnectionString =
                                                                              cc.ToString();

                                                                          //get or add based on normalized string
                                                                          return
                                                                              Clusters.GetOrAdd(
                                                                                  normalizedConnectionString,
                                                                                  new Cluster(cc));
                                                                      });

            //set the cluster
            Cluster = cluster;

            //overwrite connectionstring with normalized version
            ConnectionString = cluster.Config.ToString();

            //get a logger
            var logger = cluster.LoggerManager.GetLogger("CqlSharp.CqlConnection.Open");

            //make sure the cluster is open for connections
            await cluster.OpenAsync(logger, token).ConfigureAwait(false);

            //get a connection
            _connection = cluster.ConnectionStrategy.GetOrCreateConnection(ConnectionScope.Connection,
                                                                            PartitionKey.None);
        }

        /// <summary>
        ///   Requests a connection to be used for a command. Will reuse connection level connection if available
        /// </summary>
        /// <returns> An open connection </returns>
        internal Connection GetConnection(PartitionKey partitionKey = default(PartitionKey))
        {
            if (State != ConnectionState.Open)
                throw new CqlException("CqlConnection is not open for use");

            //reuse the connection if any available on connection level
            Connection connection = _connection ??
                                    Cluster.ConnectionStrategy.GetOrCreateConnection(ConnectionScope.Command,
                                                                                      partitionKey);

            if (connection == null)
                throw new CqlException("Unable to obtain a Cql network connection.");

            return connection;
        }

        /// <summary>
        ///   Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"> <c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources. </param>
        protected void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                Close();
                _disposed = true;
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
            return Cluster.GetPrepareResultsFor(cql);
        }
    }
}