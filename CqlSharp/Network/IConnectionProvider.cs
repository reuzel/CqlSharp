using System.Threading.Tasks;

namespace CqlSharp.Network
{
    /// <summary>
    /// Provides access to connections to cassandra node(s)
    /// </summary>
    internal interface IConnectionProvider
    {
        /// <summary>
        /// Gets or creates a network connection to a cassandra node.
        /// </summary>
        /// <returns>Connection that is ready to use</returns>
        Task<Connection> GetOrCreateConnectionAsync();

        /// <summary>
        /// Returns the connection to the provider.
        /// </summary>
        /// <param name="connection">The connection.</param>
        void ReturnConnection(Connection connection);
    }
}