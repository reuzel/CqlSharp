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
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CqlSharp.Config;

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
        private readonly ClusterConfig _config;

        private readonly ConcurrentStack<Connection> _connections;
        private readonly List<Node> _nodes;
        private readonly Random _rndGen;

        /// <summary>
        ///   Initializes the strategy with the specified nodes and cluster configuration
        /// </summary>
        /// <param name="nodes"> The nodes. </param>
        /// <param name="config"> The config. </param>
        public ExclusiveConnectionStrategy(List<Node> nodes, ClusterConfig config)
        {
            _nodes = nodes;
            _config = config;
            _connections = new ConcurrentStack<Connection>();
            _rndGen = new Random((int) DateTime.Now.Ticks);
        }

        #region IConnectionStrategy Members

        public async Task<Connection> GetOrCreateConnectionAsync()
        {
            Connection connection = null;

            //try pick an unused connection
            while (_connections.TryPop(out connection))
            {
                if (connection.IsConnected)
                    return connection;
            }

            //check if we may create another connection
            if (_config.MaxConnections <= 0 || _nodes.Sum(n => n.ConnectionCount) < _config.MaxConnections)
            {
                //all connections in use, or non available, go and create one at random node
                int count = _nodes.Count;
                int offset = _rndGen.Next(count);
                for (int i = 0; i < count; i++)
                {
                    connection = await _nodes[(offset + i)%count].CreateConnectionAsync();
                    if (connection != null)
                        return connection;
                }
            }

            //yikes nothing available
            return null;
        }

        public void ReturnConnection(Connection connection)
        {
            _connections.Push(connection);
        }

        #endregion
    }
}