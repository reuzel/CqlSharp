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

using CqlSharp.Logging;
using CqlSharp.Network;
using CqlSharp.Network.Partition;
using CqlSharp.Protocol;
using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp
{
    /// <summary>
    /// A database connection to a Cassandra Cluster
    /// </summary>
    public class CqlConnection : DbConnection
    {
        private static readonly ConcurrentDictionary<string, Cluster> Clusters;

        private Cluster _cluster;
        private Connection _connection;
        private string _database;
        private bool _disposed;
        private volatile Task _openTask;
        private CancellationTokenSource _openCancellationTokenSource;
        private string _connectionString;
        private int _connectionTimeout;

        /// <summary>
        /// Initializes the <see cref="CqlConnection"/> class.
        /// </summary>
        static CqlConnection()
        {
            Clusters = new ConcurrentDictionary<string, Cluster>();
        }


        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlConnection" /> class.
        /// </summary>
        public CqlConnection()
        {
            _connectionString = string.Empty;
            _database = string.Empty;
            _connectionTimeout = 15;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlConnection" /> class.
        /// </summary>
        /// <param name="connectionString"> The connection string. </param>
        public CqlConnection(string connectionString)
        {
            _connectionString = connectionString;
            _database = string.Empty;
            _connectionTimeout = 15;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlConnection" /> class.
        /// </summary>
        /// <param name="config"> The config. </param>
        public CqlConnection(CqlConnectionStringBuilder config)
            : this(config.ToString())
        {
        }

        /// <summary>
        /// Gets the time to wait while establishing a connection before terminating the attempt and generating an error.
        /// </summary>
        /// <returns>
        /// The time (in seconds) to wait for a connection to open. The default value is determined by the specific type of connection that you are using.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int ConnectionTimeout { get { return _connectionTimeout; } }

        /// <summary>
        /// Sets the connection timeout.
        /// </summary>
        /// <param name="timeout">The timeout in seconds</param>
        public void SetConnectionTimeout(int timeout)
        {
            _connectionTimeout = timeout < 0 ? 0 : timeout;
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
        internal CqlConnectionStringBuilder Config
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

        /// <summary>
        /// Starts a database transaction.
        /// </summary>
        /// <param name="isolationLevel">Specifies the isolation level for the transaction.</param>
        /// <returns>
        /// An object representing the new transaction.
        /// </returns>
        /// <exception cref="System.NotSupportedException"></exception>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Changes the current database for an open connection.
        /// </summary>
        /// <param name="databaseName">Specifies the name of the database for the connection to use.</param>
        /// <exception cref="System.InvalidOperationException">
        /// CqlConnection must be open before Database can be changed.
        /// or
        /// Changing a database is only supported with a ConnectionStrategy that offers exclusive connections
        /// </exception>
        public override void ChangeDatabase(string databaseName)
        {
            if (State != ConnectionState.Open)
                throw new InvalidOperationException("CqlConnection must be open before Database can be changed.");

            if (!Cluster.ConnectionStrategy.ProvidesExclusiveConnections)
                throw new InvalidOperationException(
                    "Changing a database is only supported with a ConnectionStrategy that offers exclusive connections");

            _database = databaseName;
        }

        /// <summary>
        /// Closes the connection to the database. This is the preferred method of closing any open connection.
        /// </summary>
        public override void Close()
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
        /// Gets or sets the string used to open the connection.
        /// </summary>
        /// <returns>The connection string used to establish the initial connection. The exact contents of the connection string depend on the specific data source for this connection. The default value is an empty string.</returns>
        public override string ConnectionString
        {
            get { return _connectionString; }
            set { _connectionString = value; }
        }

        /// <summary>
        /// Creates and returns a <see cref="T:CqlSharp.CqlCommand" /> object associated with the current connection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:CqlSharp.CqlCommand" /> object.
        /// </returns>
        public CqlCommand CreateCqlCommand()
        {
            return new CqlCommand(this);
        }

        /// <summary>
        /// Creates and returns a <see cref="T:System.Data.Common.DbCommand" /> object associated with the current connection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Data.Common.DbCommand" /> object.
        /// </returns>
        protected override DbCommand CreateDbCommand()
        {
            return CreateCqlCommand();
        }

        /// <summary>
        /// Gets the name of the database server to which to connect.
        /// </summary>
        /// <returns>The name of the database server to which to connect. The default value is an empty string.</returns>
        public override string DataSource
        {
            get { return Cluster.Name; }
        }

        /// <summary>
        /// Gets the name of the current database after a connection is opened, or the database name specified in the connection string before the connection is opened.
        /// </summary>
        /// <returns>The name of the current database or the name of the database to be used after a connection is opened. The default value is an empty string.</returns>
        /// <exception cref="System.ObjectDisposedException">CqlConnection</exception>
        public override string Database
        {
            get
            {
                if (_disposed)
                    throw new ObjectDisposedException("CqlConnection");

                return _database;
            }
        }

        /// <summary>
        /// Opens a database connection with the settings specified by the <see cref="P:System.Data.Common.DbConnection.ConnectionString" />.
        /// </summary>
        public override void Open()
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
        public void Cancel()
        {
            if (_openCancellationTokenSource != null)
                _openCancellationTokenSource.Cancel();
        }

        /// <summary>
        /// This is the asynchronous version of <see cref="M:System.Data.Common.DbConnection.Open" />. The cancellation token can optionally be honored.The default implementation invokes the synchronous <see cref="M:System.Data.Common.DbConnection.Open" /> call and returns a completed task. The default implementation will return a cancelled task if passed an already cancelled cancellationToken. Exceptions thrown by Open will be communicated via the returned Task Exception property.Do not invoke other methods and properties of the DbConnection object until the returned Task is complete.
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>
        /// A task representing the asynchronous operation.
        /// </returns>
        /// <exception cref="System.InvalidOperationException">Connection must be closed before it is opened</exception>
        public override Task OpenAsync(CancellationToken cancellationToken)
        {
            if (State != ConnectionState.Closed)
                throw new InvalidOperationException("Connection must be closed before it is opened");

            _openTask = OpenAsyncInternal(cancellationToken);
            return _openTask;
        }

        /// <summary>
        ///   Opens the connection.
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="System.ObjectDisposedException">CqlConnection</exception>
        private async Task OpenAsyncInternal(CancellationToken cancellationToken)
        {

            //get or add cluster based on connection string
            Cluster cluster = Clusters.GetOrAdd(ConnectionString, connString =>
            {
                //connection string unknown, create a new config 
                var cc = new CqlConnectionStringBuilder(connString);

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
            await cluster.OpenAsync(logger, cancellationToken).ConfigureAwait(false);

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
        /// Gets a string that represents the version of the server to which the object is connected.
        /// </summary>
        /// <returns>The version of the database. The format of the string returned depends on the specific type of connection you are using.</returns>
        /// <exception cref="System.NotImplementedException"></exception>
        public override string ServerVersion
        {
            get { return Cluster.CassandraVersion; }
        }

        /// <summary>
        /// Gets a string that describes the state of the connection.
        /// </summary>
        /// <returns>The state of the connection. The format of the string returned depends on the specific type of connection you are using.</returns>
        /// <exception cref="System.ObjectDisposedException">CqlConnection</exception>
        public override ConnectionState State
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
        /// Releases the unmanaged resources used by the <see cref="T:System.ComponentModel.Component" /> and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                if (State == ConnectionState.Connecting)
                {
                    try
                    {
                        //wait until open is finished (may return immediatly)
                        _openTask.Wait();
                    }
                    // ReSharper disable EmptyGeneralCatchClause
                    catch
                    {
                        //ignore here
                    }
                    // ReSharper restore EmptyGeneralCatchClause
                }

                if (State == ConnectionState.Open)
                {
                    //return connection if any
                    if (_connection != null)
                    {
                        Cluster.ConnectionStrategy.ReturnConnection(_connection, ConnectionScope.Connection);
                        _connection = null;
                    }
                }

                //clear cluster
                Cluster = null;

                _disposed = true;

            }
            base.Dispose(disposing);
        }

        internal ConcurrentDictionary<string, ResultFrame> PreparedQueryCache
        {
            get { return Cluster.PreparedQueryCache; }

        }
    }

}