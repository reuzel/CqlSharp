namespace CqlSharp.Config
{
    /// <summary>
    /// Defines the scope of the nodes to be discovered when connection to the cluster
    /// </summary>
    public enum DiscoveryScope
    {
        /// <summary>
        /// Find all nodes in the Cassandra cluster
        /// </summary>
        Cluster,

        /// <summary>
        /// Find all nodes in the racks that the configured nodes are part of
        /// </summary>
        Rack,

        /// <summary>
        /// Find all nodes in the datacenters that the configured nodes are part of
        /// </summary>
        DataCenter,

        /// <summary>
        /// Do not search for additional nodes
        /// </summary>
        None
    }
}