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

using System.Data;
using CqlSharp.Config;
using CqlSharp.Logging;
using CqlSharp.Network;
using CqlSharp.Network.Partition;
using CqlSharp.Protocol;
using System;
using System.Collections.Concurrent;
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

        void IDbConnection.ChangeDatabase(string databaseName)
        {
            throw new NotImplementedException();
        }

        void IDbConnection.Close()
        {
            //no effect, TODO: introduce more states
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

        int IDbConnection.ConnectionTimeout
        {
            get { throw new NotSupportedException(); }
        }

        public IDbCommand CreateCommand()
        {
            return new CqlCommand(this);
        }

        string IDbConnection.Database
        {
            get { throw new NotImplementedException(); }
        }

        void IDbConnection.Open()
        {
            Open();
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

            //get an exclusive connection
            if (_cluster.Config.ConnectionStrategy == ConnectionStrategy.Exclusive)
            {
                _connection = _cluster.GetOrCreateConnection(PartitionKey.None);

                if (_connection == null)
                    throw new CqlException("Unable to obtain an exclusive Cql network connection.");
            }
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
        internal Connection GetConnection(PartitionKey partitionKey = default(PartitionKey))
        {
            if (_state == 0)
                throw new CqlException("You must invoke Open or OpenAsync on a CqlConnection before other use.");

            if (_state == 2)
                throw new ObjectDisposedException("CqlConnection");

            //reuse the connection if the connection strategy is exclusive
            Connection connection = _cluster.Config.ConnectionStrategy == ConnectionStrategy.Exclusive
                                        ? _connection
                                        : _cluster.GetOrCreateConnection(partitionKey);

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
            if (disposing && (Interlocked.Exchange(ref _state, 2) != 2))
            {
                if (_connection != null)
                {
                    _cluster.ReturnConnection(_connection);
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