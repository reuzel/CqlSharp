using System;
using System.Collections.Concurrent;

namespace CqlSharp.Network
{
    /// <summary>
    /// Pool of memory buffers
    /// </summary>
    internal class MemoryPool
    {
        /// <summary>
        /// The buffer size
        /// </summary>
        public const int BufferSize = 4*1024;

        /// <summary>
        /// The max buffer pool size
        /// </summary>
        private const int MaxBufferPoolSize = 512;

        /// <summary>
        /// The singleton instance
        /// </summary>
        private static readonly Lazy<MemoryPool> TheInstance = new Lazy<MemoryPool>(() => new MemoryPool());

        /// <summary>
        /// The buffer pool
        /// </summary>
        private readonly ConcurrentBag<byte[]> _bufferPool;

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryPool" /> class.
        /// </summary>
        protected MemoryPool()
        {
            _bufferPool = new ConcurrentBag<byte[]>();
        }

        /// <summary>
        /// Gets the instance.
        /// </summary>
        /// <value>
        /// The instance.
        /// </value>
        public static MemoryPool Instance
        {
            get { return TheInstance.Value; }
        }

        /// <summary>
        /// Takes a buffer from the pool, or creates one if the pool is empty
        /// </summary>
        /// <returns>a buffer</returns>
        public byte[] Take()
        {
            byte[] buffer;
            if (!_bufferPool.TryTake(out buffer))
            {
                buffer = new byte[BufferSize];
            }

            return buffer;
        }

        /// <summary>
        /// Returns the specified buffer.
        /// </summary>
        /// <param name="buffer">The buffer.</param>
        public void Return(byte[] buffer)
        {
            if (buffer == null || buffer.Length != BufferSize)
                return;

            if (_bufferPool.Count > MaxBufferPoolSize)
                return;

            _bufferPool.Add(buffer);
        }
    }
}