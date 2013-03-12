// CqlSharp - CqlSharp
// Copyright (c) 2013 Joost Reuzel
//   
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   
// http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Concurrent;

namespace CqlSharp.Network
{
    /// <summary>
    ///   Pool of memory buffers
    /// </summary>
    internal class MemoryPool
    {
        /// <summary>
        ///   The buffer size
        /// </summary>
        public const int BufferSize = 4*1024;

        /// <summary>
        ///   The max buffer pool size
        /// </summary>
        private const int MaxBufferPoolSize = 512;

        /// <summary>
        ///   The singleton instance
        /// </summary>
        private static readonly Lazy<MemoryPool> TheInstance = new Lazy<MemoryPool>(() => new MemoryPool());

        /// <summary>
        ///   The buffer pool
        /// </summary>
        private readonly ConcurrentBag<byte[]> _bufferPool;

        /// <summary>
        ///   Initializes a new instance of the <see cref="MemoryPool" /> class.
        /// </summary>
        protected MemoryPool()
        {
            _bufferPool = new ConcurrentBag<byte[]>();
        }

        /// <summary>
        ///   Gets the instance.
        /// </summary>
        /// <value> The instance. </value>
        public static MemoryPool Instance
        {
            get { return TheInstance.Value; }
        }

        /// <summary>
        ///   Takes a buffer from the pool, or creates one if the pool is empty
        /// </summary>
        /// <returns> a buffer </returns>
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
        ///   Returns the specified buffer.
        /// </summary>
        /// <param name="buffer"> The buffer. </param>
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