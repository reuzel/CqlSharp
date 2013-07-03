using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace CqlSharp.Protocol
{
    /// <summary>
    /// Supporting class containing cached Tasks, removing the need for the creation of many...
    /// </summary>
    static class TaskCache
    {
        private const int CacheSize = 2 * 1024; //cache int and short values up to 2048

        private static readonly Task<byte>[] ByteTaskCache;

        private static readonly Task<short>[] ShortTaskCache;

        private static readonly Task<int>[] IntTaskCache;

        static TaskCache()
        {
            ByteTaskCache = new Task<byte>[256];
            for (int i = 0; i < 256; i++)
                ByteTaskCache[i] = Task.FromResult((byte)(i - Byte.MinValue));

            ShortTaskCache = new Task<short>[CacheSize + 1];
            for (short i = -1; i < CacheSize; i++)
                ShortTaskCache[i + 1] = Task.FromResult(i);

            IntTaskCache = new Task<int>[CacheSize + 1];
            for (int i = -1; i < CacheSize; i++)
                IntTaskCache[i + 1] = Task.FromResult(i);
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
            if (value >= -1 && value < CacheSize)
                result = IntTaskCache[value + 1];
            else
                result = Task.FromResult(value);

            Debug.Assert(value == result.Result, "Int value not properly cached!");

            return result;
        }

        /// <summary>
        /// returns a completed task with the given value as result
        /// </summary>
        public static Task<short> AsTask(this short value)
        {

            Task<short> result;
            if (value >= -1 && value < CacheSize)
                result = ShortTaskCache[value + 1];
            else
                result = Task.FromResult(value);

            Debug.Assert(value == result.Result, "Byte value not properly cached!");
            
            return result;
        }

    }
}
