
namespace CqlSharp.Protocol
{
    /// <summary>
    /// Consistency level for read/prepare phase of CompareAndSet (CAS) operations
    /// </summary>
    internal enum SerialConsistency : ushort
    {

        /// <summary>
        /// use the full cluster to compare values
        /// </summary>
        Serial = 0x0008,

        /// <summary>
        /// restrict the read/prepare phase to nodes in the current datacenter
        /// </summary>
        LocalSerial = 0x0009
    }
}
