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
using System.Threading;
using CqlSharp.Network.Partition;

namespace CqlSharp.Network
{
    /// <summary>
    /// Connection selection strategy that randomizes the connections over the nodes in the cluster
    /// </summary>
    internal class RandomConnectionStrategy : IConnectionStrategy
    {
        private readonly CqlConnectionStringBuilder _config;
        private readonly Ring _nodes;
        private readonly Random _rnd;
        private int _connectionCount;

        /// <summary>
        /// Initializes the strategies with the specified nodes and cluster configuration
        /// </summary>
        /// <param name="nodes"> The nodes. </param>
        /// <param name="config"> The config. </param>
        public RandomConnectionStrategy(Ring nodes, CqlConnectionStringBuilder config)
        {
            _nodes = nodes;
            _config = config;
            _rnd = new Random((int)DateTime.UtcNow.Ticks);
        }

        #region IConnectionStrategy Members

        /// <summary>
        /// Gets or creates connection to the cluster.
        /// </summary>
        /// <param name="scope"> The scope. </param>
        /// <param name="partitionKey"> The partition key. </param>
        /// <returns> </returns>
        public Connection GetOrCreateConnection(ConnectionScope scope, PartitionKey partitionKey)
        {
            //provide connections on command level only
            if(scope == ConnectionScope.Connection)
                return null;

            int count = _nodes.Count;
            int offset = _rnd.Next(count);

            Connection connection = null;

            //try to get an unused connection from a random node
            for(int i = 0; i < count; i++)
            {
                Node randomNode = _nodes[(offset + i)%count];

                //skip if node is down
                if(!randomNode.IsUp) continue;

                //try get a connection from it
                connection = randomNode.GetConnection();

                //connection is not used to the max, ok to use
                if(connection != null && connection.Load < _config.NewConnectionTreshold)
                    break;

                //get a new connection to the node if possible
                if(_config.MaxConnections <= 0 || _connectionCount < _config.MaxConnections)
                {
                    //try to get a new connection from this random node
                    Connection newConnection = randomNode.CreateConnection();

                    if(newConnection != null)
                    {
                        Interlocked.Increment(ref _connectionCount);
                        newConnection.OnConnectionChange +=
                            (c, ev) => { if(ev.Connected == false) Interlocked.Decrement(ref _connectionCount); };

                        connection = newConnection;
                        break;
                    }
                }
            }

            return connection;
        }


        /// <summary>
        /// Invoked when a connection is no longer in use by the application
        /// </summary>
        /// <param name="connection"> The connection no longer used. </param>
        /// <param name="scope"> The scope. </param>
        public void ReturnConnection(Connection connection, ConnectionScope scope)
        {
            //no-op
        }

        public bool ProvidesExclusiveConnections
        {
            get { return false; }
        }

        #endregion
    }
}