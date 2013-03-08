using System.Threading.Tasks;

namespace CqlSharp.Network
{
    internal interface IConnectionStrategy
    {
        /// <summary>
        /// Gets or creates connection to the cluster.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="CqlException">Can not connect to any node of the cluster! All connectivity to the cluster seems to be lost</exception>
        Task<Connection> GetOrCreateConnectionAsync();

        /// <summary>
        /// Invoked when a connection is no longer in use by the application
        /// </summary>
        /// <param name="connection">The connection no longer used.</param>
        void ReturnConnection(Connection connection);
    }
}