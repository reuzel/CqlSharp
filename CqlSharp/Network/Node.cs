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
using CqlSharp.Network.Partition;
using CqlSharp.Protocol;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;

namespace CqlSharp.Network
{
    /// <summary>
    ///   A single node of a Cassandra cluster. Manages a set of connections to that specific node. A node will be marked as down
    ///   when the last connection to that node fails. The node status will be reset to up using a exponantial back-off procedure.
    /// </summary>
    internal class Node : IEnumerable<Connection>
    {
        /// <summary>
        ///   lock to make connection creation/getting mutual exclusive
        /// </summary>
        private readonly ReaderWriterLockSlim _connectionLock;

        /// <summary>
        ///   The set of connections to the node
        /// </summary>
        private readonly List<Connection> _connections;

        /// <summary>
        ///   The lock used to coordinate up status
        /// </summary>
        private readonly object _statusLock;

        /// <summary>
        ///   The connection cleanup timer, used to remove idle connections
        /// </summary>
        private Timer _connectionCleanupTimer;

        /// <summary>
        ///   Counter used to give each connection a unique number (per node)
        /// </summary>
        private int _counter;

        /// <summary>
        ///   The failure count, used to (exponentially) increase the time before node is returned to up status
        /// </summary>
        private int _failureCount;

        /// <summary>
        ///   The cumulative load of the connections on this node
        /// </summary>
        private int _load;

        /// <summary>
        ///   The number of open/created connections
        /// </summary>
        private int _openConnections;

        /// <summary>
        ///   The timer used to restore the node to up status
        /// </summary>
        private Timer _reactivateTimer;

        /// <summary>
        ///   Initializes a new instance of the <see cref="Node" /> class.
        /// </summary>
        /// <param name="address"> The address of the node </param>
        /// <param name="cluster"> The cluster </param>
        public Node(IPAddress address, Cluster cluster)
        {
            PreparedQueryIds = new ConcurrentDictionary<string, byte[]>();
            _statusLock = new object();
            _connectionLock = new ReaderWriterLockSlim();
            _connections = new List<Connection>();
            Address = address;
            Cluster = cluster;
            IsUp = true;
            Tokens = new HashSet<string>();
            _counter = 0;
            FrameVersion = FrameVersion.ProtocolVersion2;
        }

        /// <summary>
        ///   Gets the cluster.
        /// </summary>
        /// <value> The cluster. </value>
        public Cluster Cluster { get; private set; }

        /// <summary>
        ///   Gets the address of the node.
        /// </summary>
        /// <value> The address. </value>
        public IPAddress Address { get; private set; }

        /// <summary>
        ///   Gets or sets the data center.
        /// </summary>
        /// <value> The data center. </value>
        public string DataCenter { get; set; }

        /// <summary>
        ///   Gets or sets the rack.
        /// </summary>
        /// <value> The rack. </value>
        public string Rack { get; set; }

        /// <summary>
        ///   Gets or sets the tokens.
        /// </summary>
        /// <value> The tokens. </value>
        public ISet<string> Tokens { get; set; }

        /// <summary>
        ///   Gets a value indicating whether this instance is up.
        /// </summary>
        /// <value> <c>true</c> if this instance is up; otherwise, <c>false</c> . </value>
        public bool IsUp { get; private set; }

        /// <summary>
        ///   Gets the cumalative load of the connections to this node/
        /// </summary>
        /// <value> The load. </value>
        public int Load
        {
            get { return _load; }
        }

        /// <summary>
        ///   Gets the connection count.
        /// </summary>
        /// <value> The connection count. </value>
        public int ConnectionCount
        {
            get { return _openConnections; }
        }

        /// <summary>
        ///   Gets the frame (protocol) version supported by this node
        /// </summary>
        /// <value> The frame version. </value>
        public FrameVersion FrameVersion { get; internal set; }

        /// <summary>
        ///   Gets the prepared query ids.
        /// </summary>
        /// <value> The prepared query ids. </value>
        public ConcurrentDictionary<string, byte[]> PreparedQueryIds { get; private set; }

        #region IEnumerable<Connection> Members

        /// <summary>
        ///   Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns> A <see cref="T:System.Collections.Generic.IEnumerator`1" /> that can be used to iterate through the collection. </returns>
        /// <filterpriority>1</filterpriority>
        public IEnumerator<Connection> GetEnumerator()
        {
            _connectionLock.EnterReadLock();
            try
            {
                var connections = new List<Connection>(_connections);
                return ((IEnumerable<Connection>)connections).GetEnumerator();
            }
            finally
            {
                _connectionLock.ExitReadLock();
            }
        }

        /// <summary>
        ///   Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns> An <see cref="T:System.Collections.IEnumerator" /> object that can be used to iterate through the collection. </returns>
        /// <filterpriority>2</filterpriority>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        /// <summary>
        ///   Gets an existing connection, or creates one if treshold is reached.
        /// </summary>
        /// <param name="partitionKey"> ignored </param>
        /// <returns> </returns>
        public Connection GetOrCreateConnection(PartitionKey partitionKey)
        {
            Connection c = GetConnection();

            //if no connection found, or connection is full
            if (c == null || c.Load > Cluster.Config.NewConnectionTreshold)
            {
                Connection newConnection = CreateConnection();

                //set connection to new connection if any
                c = newConnection ?? c;
            }

            if (c == null && IsUp)
                Logger.Current.LogWarning("Connection to {0} not available, while node is up!", Address);

            return c;
        }

        /// <summary>
        ///   Tries to get a reference to an existing connection to this node.
        /// </summary>
        /// <returns> true, if a connection is available, null otherwise </returns>
        public Connection GetConnection()
        {
            if (IsUp)
            {
                _connectionLock.EnterReadLock();
                try
                {
                    if (_openConnections > 0)
                        return _connections.Where(c => c.IsConnected).SmallestOrDefault(c => c.Load);
                }
                finally
                {
                    _connectionLock.ExitReadLock();
                }
            }

            return null;
        }

        /// <summary>
        ///   Tries to create a new connection to this node.
        /// </summary>
        /// <returns> a connected connection, or null if not possible. </returns>
        public Connection CreateConnection()
        {
            Connection connection = null;

            if (IsUp && _openConnections < Cluster.Config.MaxConnectionsPerNode)
            {
                _connectionLock.EnterWriteLock();
                try
                {
                    //double check, we may have been raced for a new connection
                    if (IsUp && _openConnections < Cluster.Config.MaxConnectionsPerNode)
                    {
                        //create new connection
                        connection = new Connection(this, _counter++);

                        //register to connection and load changes
                        connection.OnConnectionChange += ConnectionChange;
                        connection.OnLoadChange += LoadChange;

                        //assume it will succesfully open (to avoid too many connections to be opened)
                        _openConnections++;

                        //succesfull connect, add connection to list of open connections
                        _connections.Add(connection);

                        //create cleanup timer if it does not exist yet
                        if (_connectionCleanupTimer == null)
                            _connectionCleanupTimer = new Timer(RemoveIdleConnections, null,
                                                                TimeSpan.FromSeconds(Cluster.Config.MaxConnectionIdleTime),
                                                                TimeSpan.FromSeconds(Cluster.Config.MaxConnectionIdleTime));
                    }
                }
                finally
                {
                    _connectionLock.ExitWriteLock();
                }
            }

            //return connection (if any)
            return connection;
        }

        /// <summary>
        ///   Removes the idle connections.
        /// </summary>
        /// <param name="state"> The state. Unused </param>
        private void RemoveIdleConnections(object state)
        {
            var logger = Cluster.LoggerManager.GetLogger("CqlSharp.Node.IdleTimer");
            using (logger.ThreadBinding())
            {
                _connectionLock.EnterWriteLock();
                try
                {
                    //iterate over connections and remove idle ones
                    var conns = new List<Connection>(_connections);
                    foreach (Connection connection in conns)
                    {
                        if (connection.IsIdle)
                        {
                            logger.LogInfo("Closing {0} as it is idle", this);
                            connection.Dispose();
                            _connections.Remove(connection);
                        }
                    }

                    //remove clean up timer when this node does not have any connections left
                    if (_connections.Count == 0 && _connectionCleanupTimer != null)
                    {
                        _connectionCleanupTimer.Dispose();
                        _connectionCleanupTimer = null;
                    }
                }
                finally
                {
                    _connectionLock.ExitWriteLock();
                }
            }
        }


        /// <summary>
        ///   Invoked as handler when the status of a connection changes.
        /// </summary>
        /// <param name="sender"> The connection invoking this event handler </param>
        /// <param name="evt"> The event. </param>
        private void ConnectionChange(Object sender, ConnectionChangeEvent evt)
        {
            lock (_statusLock)
            {
                if (evt.Connected)
                {
                    if (!IsUp) Logger.Current.LogInfo("Node {0} is back online", Address);

                    IsUp = true;
                    _failureCount = 0;
                }
                else
                {
                    //reduce number of open connections
                    int active = Interlocked.Decrement(ref _openConnections);
                    if (active == 0 && evt.Failure)
                    {
                        //signal node failure, if this was the last open connection
                        Fail();
                    }
                }
            }
        }

        /// <summary>
        ///   Invoked as event handler when the load of connection changes
        /// </summary>
        /// <param name="sender"> The sender. </param>
        /// <param name="evt"> The evt. </param>
        private void LoadChange(Object sender, LoadChangeEvent evt)
        {
            Interlocked.Add(ref _load, evt.LoadDelta);
        }


        /// <summary>
        ///   Determines whether the specified <see cref="System.Object" /> is equal to this instance.
        /// </summary>
        /// <param name="obj"> The <see cref="System.Object" /> to compare with this instance. </param>
        /// <returns> <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, <c>false</c> . </returns>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Node)obj);
        }

        /// <summary>
        ///   Determines whether the specified <see cref="Node" /> is equal to this instance.
        /// </summary>
        /// <param name="other"> The other. </param>
        /// <returns> </returns>
        protected bool Equals(Node other)
        {
            return Equals(Address, other.Address) && Cluster.Config.Port == other.Cluster.Config.Port;
        }

        /// <summary>
        ///   Returns a hash code for this instance.
        /// </summary>
        /// <returns> A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. </returns>
        public override int GetHashCode()
        {
            unchecked
            {
                return ((Address != null ? Address.GetHashCode() : 0) * 397) ^ Cluster.Config.Port;
            }
        }

        /// <summary>
        ///   Fails this instance. Triggers the failure timer
        /// </summary>
        public void Fail()
        {
            lock (_statusLock)
            {
                //first failure, retry immediatly to make sure node is really down
                if (_failureCount == 0)
                {
                    _failureCount++;
                    return;
                }

                //we're down
                IsUp = false;

                //calculate the time, before retry
                int due = Math.Min(Cluster.Config.MaxDownTime, 2 ^ (_failureCount - 1) * Cluster.Config.MinDownTime);

                //next time wait a bit longer before accepting new connections (but not too long)
                if (due < Cluster.Config.MaxDownTime) _failureCount++;

                Logger.Current.LogInfo("{0} down, reactivating in {1}ms.", this, due);

                //set the back to live timer
                if (_reactivateTimer == null)
                    _reactivateTimer = new Timer(state => Reactivate(), this, due, Timeout.Infinite);
                else
                    _reactivateTimer.Change(due, Timeout.Infinite);
            }
        }

        /// <summary>
        ///   Reactivates this instance.
        /// </summary>
        internal void Reactivate()
        {
            var logger = Cluster.LoggerManager.GetLogger("CqlSharp.Node.Reactivate");
            lock (_statusLock)
            {
                if (!IsUp) logger.LogInfo("{0} is assumed to be available again", this);

                IsUp = true;
                _reactivateTimer.Dispose();
                _reactivateTimer = null;
            }
        }

        /// <summary>
        ///   Returns a <see cref="System.String" /> that represents this instance.
        /// </summary>
        /// <returns> A <see cref="System.String" /> that represents this instance. </returns>
        public override string ToString()
        {
            return string.Format("Node {0} (DC:{1} Rack:{2})", Address, DataCenter, Rack);
        }
    }
}