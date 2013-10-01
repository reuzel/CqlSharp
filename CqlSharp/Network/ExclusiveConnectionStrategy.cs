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
using CqlSharp.Network.Partition;

namespace CqlSharp.Network
{
    /// <summary>
    ///   This strategy will provide connections that are not shared between CqlConnection, or CqlCommand instances. This makes
    ///   interference between sequences of queries not possible. This may be of importance when "use" queries are applied. Because of
    ///   the exclusive use of connections, connections may not be used fully, and the chance that the application will run out of 
    ///   available connections is not unlikely under stress.
    /// </summary>
    internal class ExclusiveConnectionStrategy : IConnectionStrategy
    {
        private readonly CqlConnectionStringBuilder _config;

        private readonly ConcurrentStack<Connection> _connections;
        private readonly Ring _nodes;
        private readonly Random _rndGen;
        private int _connectionCount;

        /// <summary>
        ///   Initializes the strategy with the specified nodes and cluster configuration
        /// </summary>
        /// <param name="nodes"> The nodes. </param>
        /// <param name="config"> The config. </param>
        public ExclusiveConnectionStrategy(Ring nodes, CqlConnectionStringBuilder config)
        {
            _nodes = nodes;
            _config = config;
            _connections = new ConcurrentStack<Connection>();
            _rndGen = new Random((int)DateTime.UtcNow.Ticks);
            _connectionCount = 0;
        }

        #region IConnectionStrategy Members

        public Connection GetOrCreateConnection(ConnectionScope scope, PartitionKey partitionKey)
        {
            Connection connection = null;

            //provide connections on connection level only
            if (scope == ConnectionScope.Command)
                return null;

            if (scope == ConnectionScope.Infrastructure)
            {
                //connection exist, go and find random one
                if (_connectionCount > 0)
                {
                    int count = _nodes.Count;
                    int offset = _rndGen.Next(count);
                    for (int i = 0; i < count; i++)
                    {
                        connection = _nodes[(offset + i)%count].GetConnection();
                        if (connection != null)
                            return connection;
                    }
                }
            }
            else
            {
                //try pick an unused connection
                while (_connections.TryPop(out connection))
                {
                    if (connection.IsConnected)
                    {
                        connection.AllowCleanup = false;
                        return connection;
                }
            }
            }

            //check if we may create another connection if we didn't find a connection yet
            if (_config.MaxConnections <= 0 || _connectionCount < _config.MaxConnections)
            {
                //all connections in use, or non available, go and create one at random node
                int count = _nodes.Count;
                int offset = _rndGen.Next(count);
                for (int i = 0; i < count; i++)
                {
                    connection = _nodes[(offset + i)%count].CreateConnection();
                    if (connection != null)
                    {
                        Interlocked.Increment(ref _connectionCount);
                        connection.OnConnectionChange += (src, ev) =>
                                                             {
                                                                 if (!ev.Connected)
                                                                     Interlocked.Decrement(ref _connectionCount);
                                                             };

                        //if infrastructure scope, push connection to list of available connections for other use
                        if (scope == ConnectionScope.Infrastructure)
                            _connections.Push(connection);
                        else
                            //disable cleanup of this connection while it is in reserved for exclusive use
                            connection.AllowCleanup = false;


                        return connection;
                    }
                }
            }

            //yikes nothing available
            return connection;
        }

        public void ReturnConnection(Connection connection, ConnectionScope scope)
        {
            connection.AllowCleanup = true;
            _connections.Push(connection);
        }

        public bool ProvidesExclusiveConnections
        {
            get { return true; }
        }

        #endregion
    }
}