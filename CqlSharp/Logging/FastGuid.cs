// CqlSharp - CqlSharp
// Copyright (c) 2014 Joost Reuzel
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
using System.Threading;

namespace CqlSharp.Logging
{
    /// <summary>
    /// Faster way to create GUIDs at the cost of loss of some randomness
    /// </summary>
    internal static class FastGuid
    {
        private static readonly int A;
        private static readonly short B;
        private static readonly short C;
        private static readonly byte D;
        private static readonly byte E;

        private static long _counter;

        /// <summary>
        /// Initializes the <see cref="FastGuid" /> class with the information of single Guid.
        /// </summary>
        static FastGuid()
        {
            var baseGuid = Guid.NewGuid();
            byte[] b = baseGuid.ToByteArray();
            A = b[3] << 24 | b[2] << 16 | b[1] << 8 | b[0];
            B = (short)(b[5] << 8 | b[4]);
            C = (short)(b[7] << 8 | b[6]);
            D = b[8];
            E = b[9];
            _counter = b[10] << 40 | b[11] << 32 | b[12] << 24 | b[13] << 16 | b[14] << 8 | b[15];
        }

        /// <summary>
        /// Creates a new unique identifier.
        /// </summary>
        /// <returns> </returns>
        public static Guid NewGuid()
        {
            long tick = Interlocked.Increment(ref _counter);

            var f = (byte)(tick >> 40);
            var g = (byte)(tick >> 32);
            var h = (byte)(tick >> 24);
            var i = (byte)(tick >> 16);
            var j = (byte)(tick >> 8);
            var k = (byte)(tick);

            return new Guid(A, B, C, D, E, f, g, h, i, j, k);
        }
    }
}