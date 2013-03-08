using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using CqlSharp.Config;

namespace CqlSharp.Network
{
    /// <summary>
    /// A single node of a Cassandra cluster. Manages a set of connections to that specific node. A node will be marked as down
    /// when the last connection to that node fails. The node status will be reset to up using a exponantial back-off procedure.
    /// </summary>
    internal class Node : IConnectionProvider
    {
        /// <summary>
        /// The cluster configuration
        /// </summary>
        private readonly ClusterConfig _config;

        /// <summary>
        /// lock to make connection creation/getting mutual exclusive
        /// </summary>
        private readonly SemaphoreSlim _connectionLock;

        /// <summary>
        /// The set of connections to the node
        /// </summary>
        private readonly List<Connection> _connections;

        /// <summary>
        /// The lock used to coordinate up status
        /// </summary>
        private readonly object _statusLock;

        /// <summary>
        /// The connection cleanup timer, used to remove idle connections
        /// </summary>
        private Timer _connectionCleanupTimer;

        /// <summary>
        /// The failure count, used to (exponentially) increase the time before node is returned to up status
        /// </summary>
        private int _failureCount;

        /// <summary>
        /// The cumulative load of the connections on this node
        /// </summary>
        private int _load;

        /// <summary>
        /// The number of open/created connections
        /// </summary>
        private int _openConnections;

        /// <summary>
        /// The timer used to restore the node to up status
        /// </summary>
        private Timer _reactivateTimer;

        /// <summary>
        /// Initializes a new instance of the <see cref="Node" /> class.
        /// </summary>
        /// <param name="address">The address of the node</param>
        /// <param name="config">The cluster config</param>
        public Node(IPAddress address, ClusterConfig config)
        {
            _statusLock = new object();
            _connectionLock = new SemaphoreSlim(1);
            _connections = new List<Connection>();
            Address = address;
            _config = config;
            IsUp = true;
        }

        /// <summary>
        /// Gets the address of the node.
        /// </summary>
        /// <value>
        /// The address.
        /// </value>
        public IPAddress Address { get; private set; }

        /// <summary>
        /// Gets a value indicating whether this instance is up.
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance is up; otherwise, <c>false</c>.
        /// </value>
        public bool IsUp { get; private set; }

        /// <summary>
        /// Gets the cumalative load of the connections to this node/
        /// </summary>
        /// <value>
        /// The load.
        /// </value>
        public int Load
        {
            get { return _load; }
        }

        /// <summary>
        /// Gets the connection count.
        /// </summary>
        /// <value>
        /// The connection count.
        /// </value>
        public int ConnectionCount
        {
            get { return _openConnections; }
        }

        #region IConnectionProvider Members

        /// <summary>
        /// Gets an existing connection, or creates one if treshold is reached.
        /// </summary>
        /// <returns></returns>
        public async Task<Connection> GetOrCreateConnectionAsync()
        {
            Connection c = GetConnection();

            //if no connection found, or connection is full
            if (c == null || c.Load > _config.NewConnectionTreshold)
            {
                Connection newConnection = await CreateConnectionAsync();

                //set connection to new connection if any
                c = newConnection ?? c;
            }

            return c;
        }

        public void ReturnConnection(Connection connection)
        {
            //no-op
        }

        #endregion

        /// <summary>
        /// Fails this instance. Triggers the failure timer
        /// </summary>
        public void Fail()
        {
            lock (_statusLock)
            {
                //we're down
                IsUp = false;

                //calculate the time, before retry
                int due = Math.Min(_config.MaxDownTime, 2 ^ (_failureCount)*_config.MinDownTime);

                //next time wait a bit longer before accepting new connections (but not too long)
                if (due < _config.MaxDownTime) _failureCount++;

                //set the back to live timer
                if (_reactivateTimer == null)
                    _reactivateTimer = new Timer(Reactivate, this, due, Timeout.Infinite);
                else
                    _reactivateTimer.Change(due, Timeout.Infinite);
            }
        }

        /// <summary>
        /// Reactivates this instance. State is required for event handling but ignored
        /// </summary>
        /// <param name="state">Ignored</param>
        private void Reactivate(object state)
        {
            lock (_statusLock)
            {
                IsUp = true;
                _reactivateTimer.Dispose();
                _reactivateTimer = null;
            }
        }

        /// <summary>
        /// Tries to get a reference to an existing connection to this node.
        /// </summary>
        /// <returns>true, if a connection is available, null otherwise</returns>
        public Connection GetConnection()
        {
            if (IsUp)
            {
                _connectionLock.Wait();
                try
                {
                    if (_openConnections > 0)
                        return _connections.Where(c => c.IsConnected).SmallestOrDefault(c => c.Load);
                }
                finally
                {
                    _connectionLock.Release();
                }
            }

            return null;
        }

        /// <summary>
        /// Tries to create a new connection to this node.
        /// </summary>
        /// <returns>a connected connection, or null if not possible.</returns>
        public async Task<Connection> CreateConnectionAsync()
        {
            if (IsUp)
            {
                await _connectionLock.WaitAsync();
                try
                {
                    if (_openConnections < _config.MaxConnectionsPerNode)
                    {
                        //create new connection
                        var connection = new Connection(Address, _config);

                        //increment active connection counter
                        Interlocked.Increment(ref _openConnections);

                        //register to connection and load changes
                        connection.OnConnectionChange += ConnectionChange;
                        connection.OnLoadChange += LoadChange;

                        //attempt to connect
                        await connection.ConnectAsync();

                        //succesfull connect, add connection to list of open connections
                        _connections.Add(connection);

                        //create cleanup timer if it does not exist yet
                        if (_connectionCleanupTimer == null)
                            _connectionCleanupTimer = new Timer(RemoveIdleConnections, null,
                                                                _config.MaxConnectionIdleTime,
                                                                _config.MaxConnectionIdleTime);

                        return connection;
                    }
                }
                finally
                {
                    _connectionLock.Release();
                }
            }

            return null;
        }

        /// <summary>
        /// Removes the idle connections.
        /// </summary>
        /// <param name="state">The state. Unused</param>
        private void RemoveIdleConnections(object state)
        {
            _connectionLock.Wait();
            try
            {
                //iterate over connections and remove idle ones
                var conns = new List<Connection>(_connections);
                foreach (Connection connection in conns)
                {
                    if (connection.IsIdle)
                    {
                        connection.Dispose();
                        _connections.Remove(connection);
                    }
                }

                //remove clean up timer when this node does not have any connections left
                if (_connections.Count == 0)
                {
                    _connectionCleanupTimer.Dispose();
                    _connectionCleanupTimer = null;
                }
            }
            finally
            {
                _connectionLock.Release();
            }
        }


        /// <summary>
        /// Invoked as handler when the status of a connection changes.
        /// </summary>
        /// <param name="sender">The connection invoking this event handler</param>
        /// <param name="evt">The event.</param>
        private void ConnectionChange(Object sender, ConnectionChangeEvent evt)
        {
            lock (_statusLock)
            {
                if (evt.Connected)
                {
                    IsUp = true;
                    _failureCount = 0;
                }
                else
                {
                    //reduce number of connections
                    int active = Interlocked.Decrement(ref _openConnections);
                    if (active == 0 && evt.Failure)
                    {
                        //signal failure, if this was the last open connection
                        Fail();
                    }
                }
            }
        }

        /// <summary>
        /// Invoked as event handler when the load of connection changes
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="evt">The evt.</param>
        private void LoadChange(Object sender, LoadChangeEvent evt)
        {
            Interlocked.Add(ref _load, evt.LoadDelta);
        }
    }
}