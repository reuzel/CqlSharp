
namespace CqlSharp
{
    /// <summary>
    /// Structure to capture options to use for executing the query
    /// </summary>
    public class CqlExecutionOptions
    {
        public static readonly CqlExecutionOptions None = new CqlExecutionOptions() { UseBuffering = false, TracingEnabled = false, UseParallelConnections = false };

        public CqlExecutionOptions()
        {
            UseBuffering = false;
            TracingEnabled = false;
            UseParallelConnections = false;
        }

        /// <summary>
        /// Gets or sets a value indicating whether to use response buffering.
        /// </summary>
        /// <value>
        ///   <c>true</c> if buffering should be used; otherwise, <c>false</c>.
        /// </value>
        public bool UseBuffering { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether tracing enabled should be enabled.
        /// </summary>
        /// <value>
        ///   <c>true</c> if tracing enabled; otherwise, <c>false</c>.
        /// </value>
        public bool TracingEnabled { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the query is allowed to use different connection, than the default connection from the used CqlConnection.
        /// </summary>
        /// <value>
        /// <c>true</c> if parallel connections are allowed to be used; otherwise, <c>false</c>.
        /// </value>
        public bool UseParallelConnections { get; set; }

    }
}
