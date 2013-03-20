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
using CqlSharp.Network.Partition;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace CqlSharp.Network
{
    /// <summary>
    ///   Connection selection strategy that randomizes the connections over the nodes in the cluster
    /// </summary>
    internal class RandomConnectionStrategy : IConnectionStrategy
    {
        private readonly ClusterConfig _config;
        private readonly Ring _nodes;
        private readonly Random _rnd;

        /// <summary>
        ///   Initializes the strategies with the specified nodes and cluster configuration
        /// </summary>
        /// <param name="nodes"> The nodes. </param>
        /// <param name="config"> The config. </param>
        public RandomConnectionStrategy(Ring nodes, ClusterConfig config)
        {
            _nodes = nodes;
            _config = config;
            _rnd = new Random((int)DateTime.Now.Ticks);
        }

        #region IConnectionStrategy Members

        /// <summary>
        ///   Gets or creates connection to the cluster.
        /// </summary>
        /// <param name="partitionKey"> </param>
        /// <returns> </returns>
        /// <exception cref="CqlException">Can not connect to any node of the cluster! All connectivity to the cluster seems to be lost</exception>
        public async Task<Connection> GetOrCreateConnectionAsync(PartitionKey partitionKey)
        {
            Connection connection;
            int count = _nodes.Count;
            int offset = _rnd.Next(count);

            //try to get an unused connection from a random node
            for (int i = 0; i < count; i++)
            {
                try
                {
                    connection = _nodes[(offset + i) % count].GetConnection();
                    if (connection != null && connection.Load < _config.NewConnectionTreshold)
                        return connection;
                }
                catch
                {
                    //ignore, try another node
                }
            }

            //check if we may create another connection
            if (_config.MaxConnections <= 0 || _nodes.Sum(n => n.ConnectionCount) < _config.MaxConnections)
            {
                //iterate over nodes and try to create a new one
                for (int i = 0; i < count; i++)
                {
                    try
                    {
                        connection = await _nodes[(offset + i) % count].CreateConnectionAsync();
                        if (connection != null)
                            return connection;
                    }
                    catch
                    {
                        //ignore, try another node
                    }
                }
            }

            //iterate over nodes and get an existing one
            for (int i = 0; i < count; i++)
            {
                connection = _nodes[(offset + i) % count].GetConnection();
                if (connection != null)
                    return connection;
            }

            return null;
        }

        /// <summary>
        ///   Invoked when a connection is no longer in use by the application
        /// </summary>
        /// <param name="connection"> The connection no longer used. </param>
        public void ReturnConnection(Connection connection)
        {
            //no-op
        }

        #endregion
    }
}