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
using System.Text;

namespace CqlSharp.Memory
{
    /// <summary>
    ///   Pool of memory buffers
    /// </summary>
    internal class MemoryPool
    {
        private readonly byte[] _emptyArray = new byte[0];

        /// <summary>
        /// Number of pools, each pool in exponential size
        /// </summary>
        private const int MaxPools = 7;

        /// <summary>
        ///   The max buffer pool items, per size
        /// </summary>
        private const int MaxBufferPoolItems = 32;

        /// <summary>
        ///   The singleton instance
        /// </summary>
        private static readonly Lazy<MemoryPool> TheInstance = new Lazy<MemoryPool>(() => new MemoryPool());

        /// <summary>
        ///   The buffer pools
        /// </summary>
        private readonly ConcurrentQueue<byte[]>[] _bufferPools;
        private readonly int[] _sizes;

        /// <summary>
        ///   Initializes a new instance of the <see cref="MemoryPool" /> class.
        /// </summary>
        protected MemoryPool()
        {
            _bufferPools = new ConcurrentQueue<byte[]>[MaxPools];
            _sizes = new int[MaxPools];
            for (int i = 0; i < MaxPools; i++)
            {
                _bufferPools[i] = new ConcurrentQueue<byte[]>();
                _sizes[i] = (int)Math.Pow(2, i) * 1024;
            }
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
        ///   Takes a buffer from the pool, or creates one if the pool is empty, or requested size is larger than BufferSize
        /// </summary>
        /// <returns> a buffer </returns>
        public byte[] Take(int size)
        {
            if (size == 0)
                return _emptyArray;

            byte[] buffer;

            int bufferIndex = Array.BinarySearch(_sizes, size);
            bufferIndex = bufferIndex < 0 ? ~bufferIndex : bufferIndex;

            if (bufferIndex >= MaxPools)
            {
                buffer = new byte[size];
            }
            else if (!_bufferPools[bufferIndex].TryDequeue(out buffer))
            {
                buffer = new byte[_sizes[bufferIndex]];
            }

            return buffer;
        }

        /// <summary>
        ///   Returns the specified buffer.
        /// </summary>
        /// <param name="buffer"> The buffer. </param>
        public void Return(byte[] buffer)
        {
            int bufferIndex = Array.BinarySearch(_sizes, buffer.Length);
            if (bufferIndex >= 0 && _bufferPools[bufferIndex].Count < MaxBufferPoolItems)
            {
                _bufferPools[bufferIndex].Enqueue(buffer);
            }
        }

        public override string ToString()
        {
            var builder = new StringBuilder();
            builder.Append("MemoryPool(");
            for (int i = 0; i < MaxPools; i++)
            {
                builder.Append(_sizes[i]);
                builder.Append(":");
                builder.Append(_bufferPools[i].Count);
                if (i < MaxPools - 1)
                    builder.Append("; ");
            }
            builder.Append(")");
            return builder.ToString();
        }
    }
}