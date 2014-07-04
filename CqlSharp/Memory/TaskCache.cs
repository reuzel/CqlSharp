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
using System.Diagnostics;
using System.Threading.Tasks;

namespace CqlSharp.Memory
{
    /// <summary>
    /// Supporting class containing cached Tasks, removing the need for the creation of many...
    /// </summary>
    internal static class TaskCache
    {
        private const int CacheSize = 2*1024; //cache int and short values up to 2048

        private static readonly Task<byte>[] ByteTaskCache;

        private static readonly Task<ushort>[] ShortTaskCache;

        private static readonly Task<int>[] IntTaskCache;

        public static readonly Task CompletedTask;

        static TaskCache()
        {
            ByteTaskCache = new Task<byte>[256];
            for(int i = 0; i < 256; i++)
            {
                ByteTaskCache[i] = Task.FromResult((byte)(i - Byte.MinValue));
            }

            ShortTaskCache = new Task<ushort>[CacheSize];
            for(ushort i = 0; i < CacheSize; i++)
            {
                ShortTaskCache[i] = Task.FromResult(i);
            }

            IntTaskCache = new Task<int>[CacheSize + 1];
            for(int i = -1; i < CacheSize; i++)
            {
                IntTaskCache[i + 1] = Task.FromResult(i);
            }

            CompletedTask = Task.FromResult(true);
        }

        /// <summary>
        /// returns a completed task with the given value as result
        /// </summary>
        public static Task<byte> AsTask(this byte value)
        {
            Task<byte> result = ByteTaskCache[value + Byte.MinValue];
            Debug.Assert(value == result.Result, "Byte value not properly cached!");
            return result;
        }

        /// <summary>
        /// returns a completed task with the given value as result
        /// </summary>
        public static Task<int> AsTask(this int value)
        {
            Task<int> result;
            if(value >= -1 && value < CacheSize)
                result = IntTaskCache[value + 1];
            else
                result = Task.FromResult(value);

            Debug.Assert(value == result.Result, "Int value not properly cached!");

            return result;
        }

        /// <summary>
        /// returns a completed task with the given value as result
        /// </summary>
        public static Task<ushort> AsTask(this ushort value)
        {
            Task<ushort> result = value < CacheSize ? ShortTaskCache[value] : Task.FromResult(value);

            Debug.Assert(value == result.Result, "Short value not properly cached!");

            return result;
        }
    }
}