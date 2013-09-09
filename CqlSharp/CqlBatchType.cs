namespace CqlSharp
{
    /// <summary>
    /// Types of batch consistency
    /// </summary>
    public enum CqlBatchType : byte
    {
        Logged = 0,
        Unlogged = 1,
        Counter = 2
    }
}