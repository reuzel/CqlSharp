namespace CqlSharp.Config
{
    /// <summary>
    /// Strategy options for managing connections to the cluster
    /// </summary>
    public enum ConnectionStrategy
    {
        /// <summary>
        /// The balanced strategy. Attempts to spread queries over connections based on their load indication
        /// </summary>
        Balanced,

        /// <summary>
        /// The random strategy. Spreads load, by randomizing access to nodes
        /// </summary>
        Random
    }
}