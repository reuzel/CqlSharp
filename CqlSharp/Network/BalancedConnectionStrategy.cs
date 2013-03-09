using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CqlSharp.Config;

namespace CqlSharp.Network
{
    /// <summary>
    /// This implementation attempts to balance the connections over the cluster based on load. First it will
    /// try to reuse an existing connection. If no connections exist, or if all connection loads are larger than
    /// the newConnectionTreshold, a new connection is created at a node with an as low as possible load. If that fails
    /// (e.g. because the max amount of connections per node is reached), an attempt is made to select the least used
    /// connection from the least used node.
    /// </summary>
    internal class BalancedConnectionStrategy : IConnectionStrategy
    {
        private readonly ClusterConfig _config;
        private readonly List<Node> _nodes;

        /// <summary>
        /// Initializes the strategy with the specified nodes and cluster configuration
        /// </summary>
        /// <param name="nodes">The nodes.</param>
        /// <param name="config">The config.</param>
        public BalancedConnectionStrategy(List<Node> nodes, ClusterConfig config)
        {
            _nodes = nodes;
            _config = config;
        }

        #region IConnectionStrategy Members

        /// <summary>
        /// Gets or creates connection to the cluster.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="CqlException">Can not connect to any node of the cluster! All connectivity to the cluster seems to be lost</exception>
        public async Task<Connection> GetOrCreateConnectionAsync()
        {
            //Sort the nodes by load (unused first)
            var nodesByLoad = new List<Node>(_nodes.Where(n => n.IsUp).OrderBy(n => n.Load));

            //find the least used connection per node and see if it can be used
            foreach (Node node in nodesByLoad)
            {
                Connection connection = node.GetConnection();
                if (connection != null && connection.Load < _config.NewConnectionTreshold)
                    return connection;
            }

            //check if we may create another connection
            foreach (Node node in nodesByLoad)
            {
                try
                {
                    Connection connection = await node.CreateConnectionAsync();
                    if (connection != null)
                        return connection;
                }
                    // ReSharper disable EmptyGeneralCatchClause
                catch
                {
                    //ignore, errors handled within node, try to create another one at the next node
                }
                // ReSharper restore EmptyGeneralCatchClause
            }

            //no suitable connection found or created, go to the least used node, and pick its least used open connection...
            foreach (Node node in nodesByLoad)
            {
                Connection connection = node.GetConnection();
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
            //connections are shared, nothing to do here
        }

        #endregion
    }
}