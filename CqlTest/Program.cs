// CqlSharp - CqlTest
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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CqlTest
{
    /// <summary>
    ///   Utility program that executes a set of tests. Usefull for profiling purposes...
    /// </summary>
    internal class Program
    {
        private int _count;

        private string[] _groups;
        private CartManager _manager;
        private int _queries;
        private Stopwatch _stopwatch;
        private ConcurrentDictionary<string, LinkedList<long>> _times;

        public async Task DoAddCarts()
        {
            var list = _times.GetOrAdd("addCart", n => new LinkedList<long>());

            int groupId = 0;
            int nr = Interlocked.Increment(ref _count);

            while (nr <= _queries)
            {
                long start = _stopwatch.ElapsedMilliseconds;
                await _manager.AddCartAsync(_groups[groupId]);
                long stop = _stopwatch.ElapsedMilliseconds;
                lock (list) list.AddLast(stop - start);
                groupId = (groupId + 1)%_groups.Length;
                nr = Interlocked.Increment(ref _count);
            }
        }

        public async Task DoUpdateCarts(Cart[] carts)
        {
            var list = _times.GetOrAdd("updateCart", n => new LinkedList<long>());

            int groupId = 0;
            int cartIndex = 0;
            int nr = Interlocked.Increment(ref _count);
            while (nr <= _queries)
            {
                Cart cart = carts[cartIndex];
                cart.GroupId = _groups[groupId];
                cart.Items = new Dictionary<string, int> {{"item1", nr}, {"item2", nr}};

                long start = _stopwatch.ElapsedMilliseconds;

                await _manager.UpdateCartAsync(cart);

                long stop = _stopwatch.ElapsedMilliseconds;

                lock (list) list.AddLast(stop - start);

                groupId = (groupId + 1)%_groups.Length;
                cartIndex = (cartIndex + 1)%carts.Length;
                nr = Interlocked.Increment(ref _count);
            }
        }

        public async Task DoAddItems(Cart[] carts)
        {
            var list = _times.GetOrAdd("addItems", n => new LinkedList<long>());

            int cartIndex = 0;
            int nr = Interlocked.Increment(ref _count);
            while (nr <= _queries)
            {
                Cart cart = carts[cartIndex];
                var items = new Dictionary<string, int> {{"item1_" + nr, nr}, {"item2_" + nr, nr}};

                long start = _stopwatch.ElapsedMilliseconds;

                await _manager.AddItemsAsync(cart.Id, items);

                long stop = _stopwatch.ElapsedMilliseconds;

                lock (list) list.AddLast(stop - start);

                cartIndex = (cartIndex + 1)%carts.Length;
                nr = Interlocked.Increment(ref _count);
            }
        }

        public async Task DoGetItems(Cart[] carts)
        {
            var list = _times.GetOrAdd("getItems", n => new LinkedList<long>());

            int cartIndex = 0;
            int nr = Interlocked.Increment(ref _count);
            while (nr <= _queries)
            {
                Cart cart = carts[cartIndex];

                long start = _stopwatch.ElapsedMilliseconds;

                await _manager.GetItemsAsync(cart.Id);

                long stop = _stopwatch.ElapsedMilliseconds;

                lock (list) list.AddLast(stop - start);

                cartIndex = (cartIndex + 1)%carts.Length;
                nr = Interlocked.Increment(ref _count);
            }
        }

        public async Task DoFindByGroupId()
        {
            var list = _times.GetOrAdd("findByGroupId", n => new LinkedList<long>());

            int groupId = 0;
            int nr = Interlocked.Increment(ref _count);
            while (nr <= _queries)
            {
                long start = _stopwatch.ElapsedMilliseconds;
                await _manager.FindCartsByGroupIdAsync(_groups[groupId]);
                long stop = _stopwatch.ElapsedMilliseconds;

                lock (list) list.AddLast(stop - start);

                groupId = (groupId + 1)%_groups.Length;
                nr = Interlocked.Increment(ref _count);
            }
        }

        public void Run(int queries, int threads, int prepared)
        {
            Console.WriteLine("Preparing...");
            _queries = queries;
            _manager = new CartManager();
            _times = new ConcurrentDictionary<string, LinkedList<long>>();
            _stopwatch = new Stopwatch();
            _count = 0;
            _groups = Enumerable.Range(0, 9).Select(n => "group_" + n).ToArray();


            //pregenerate carts
            _manager.PrepareDbAsync().Wait();
            var cartTasks = new Task<Cart>[prepared];
            int groupId = 0;
            for (int i = 0; i < prepared; i++)
            {
                cartTasks[i] = _manager.AddCartAsync(_groups[groupId]);
                groupId = (groupId + 1)%_groups.Length;
            }

            Task.WaitAll(cartTasks);
            var carts = cartTasks.Select(t => t.Result).ToArray();

            Console.WriteLine("Executing...");

            //execute!
            _stopwatch.Start();
            var doTasks = new LinkedList<Task>();

            for (int i = 0; i < threads; i++)
            {
                doTasks.AddLast(DoAddCarts());
                doTasks.AddLast(DoAddItems(carts));
                //doTasks.AddLast(DoFindByGroupId());
                doTasks.AddLast(DoGetItems(carts));
                doTasks.AddLast(DoUpdateCarts(carts));
            }

            Task.WaitAll(doTasks.ToArray());

            _stopwatch.Stop();

            //print results
            Console.WriteLine("Total tasks run: {0} in {1} ({2} req/s)", queries, _stopwatch.Elapsed,
                              DoubleString((double) queries/_stopwatch.ElapsedMilliseconds*1000));

#if debug
            Console.WriteLine(MemoryPool.Instance);
#endif
            Console.WriteLine();
            WriteRow("", "Calls", "Avg", "Median", "Min", "Max");
            WriteStatistics("Total", _times.SelectMany(vls => vls.Value));
            Console.WriteLine(new string('-', 60));
            foreach (var call in _times)
            {
                WriteStatistics(call.Key, call.Value);
            }
        }

        public string DoubleString(double value)
        {
            return value.ToString("F2");
        }

        public void WriteRow(params object[] columns)
        {
            Console.WriteLine("{0,-20} {1,-7} {2,-7} {3,-7} {4,-7} {5,-7}", columns);
        }

        public void WriteStatistics(string name, IEnumerable<long> values)
        {
            var array = values.ToArray();
            Array.Sort(array);

            long median;
            if (array.Length%2 == 1)
            {
                median = array[array.Length/2 + 1];
            }
            else
            {
                median = (array[array.Length/2] + array[array.Length/2 + 1])/2;
            }

            WriteRow(name, array.Length, DoubleString(array.Average()), DoubleString(median), DoubleString(array[0]),
                     DoubleString(array[array.Length - 1]));
        }

        public static void Main(string[] args)
        {
            const int prepared = 10000;
            const int queries = 50000;
            const int threads = 4;

            var program = new Program();
            program.Run(queries, threads, prepared);

            //Console.ReadLine();
        }
    }

    public static class Extensions
    {
        public static long Median(this IEnumerable<long> values)
        {
            var array = values.ToArray();
            Array.Sort(array);
            if (array.Length%2 == 1)
            {
                return array[array.Length/2 + 1];
            }

            return (array[array.Length/2] + array[array.Length/2 + 1])/2;
        }
    }
}