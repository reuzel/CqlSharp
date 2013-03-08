using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using CqlSharp.Config;

namespace CqlSharp.Network
{
    /// <summary>
    /// Connection selection strategy that randomizes the connections over the nodes in the cluster
    /// </summary>
    internal class RandomConnectionStrategy : IConnectionStrategy
    {
        private readonly ConcurrentStack<Connection> _connections;
        private readonly List<Node> _nodes;
        private readonly Random _rnd;
        private ClusterConfig _config;

        /// <summary>
        /// Initializes the strategies with the specified nodes and cluster configuration
        /// </summary>
        /// <param name="nodes">The nodes.</param>
        /// <param name="config">The config.</param>
        public RandomConnectionStrategy(List<Node> nodes, ClusterConfig config)
        {
            _nodes = nodes;
            _config = config;
            _connections = new ConcurrentStack<Connection>();
            _rnd = new Random((int) DateTime.Now.Ticks);
        }

        #region IConnectionStrategy Members

        /// <summary>
        /// Gets or creates connection to the cluster.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="CqlException">Can not connect to any node of the cluster! All connectivity to the cluster seems to be lost</exception>
        public async Task<Connection> GetOrCreateConnectionAsync()
        {
            Connection connection;

            //try to get an unused connection
            while (_connections.TryPop(out connection))
            {
                if (connection.IsConnected)
                {
                    return connection;
                }
            }

            //iterate over nodes and try to create a new one
            int count = _nodes.Count;
            int nodeIndex = _rnd.Next(0, count);
            for (int i = 0; i < count; i++)
            {
                try
                {
                    connection = await _nodes[nodeIndex + i%count].CreateConnectionAsync();
                    if (connection != null)
                        return connection;
                }
                catch
                {
                    //ignore, try another node
                }
            }

            //iterate over nodes and get an existing one
            for (int i = 0; i < count; i++)
            {
                connection = _nodes[nodeIndex + i%count].GetConnection();
                if (connection != null)
                    return connection;
            }

            return null;
        }

        /// <summary>
        /// Invoked when a connection is no longer in use by the application
        /// </summary>
        /// <param name="connection">The connection no longer used.</param>
        public void ReturnConnection(Connection connection)
        {
            _connections.Push(connection);
        }

        #endregion
    }
}