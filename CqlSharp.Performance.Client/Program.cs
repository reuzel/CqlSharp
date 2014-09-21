// CqlSharp - CqlSharp.Performance.Client
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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CommandLine;

namespace CqlSharp.Performance.Client
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var options = new Options();
            if(!Parser.Default.ParseArguments(args, options))
                return;

            Func<string, Task> request = options.Sync ? (Func<string, Task>)DoRequest : DoRequestAsync;

            Console.WriteLine("Waiting for server to start...");
            Thread.Sleep(10000);

            ThreadPool.SetMinThreads(options.Concurrent + 10, options.Concurrent + 10);

            Console.WriteLine("Sending warmup request");
            DoRequest(options.Server).Wait();
            Thread.Sleep(5000);

            Console.WriteLine("Starting...");

            var st = new Stopwatch();
            st.Start();

            var times = Run(options.Concurrent, options.Requests, request, options.Server).Result;

            st.Stop();
            times.Sort();

            Console.WriteLine();
            Console.WriteLine("== [OVERALL] =============================");
            Console.WriteLine("API:          {0}", options.Sync ? "Sync" : "Async");
            Console.WriteLine("Concurrent:   {0}", options.Concurrent);
            Console.WriteLine("Total Time:   {0}ms", st.ElapsedMilliseconds);
            Console.WriteLine("Requests:     {0}", options.Requests);
            Console.WriteLine("Requests/sec: {0}", (int)((double)options.Requests/st.ElapsedMilliseconds*1000));
            Console.WriteLine("== [TIMING] ==============================");
            Console.WriteLine("Minimum:      {0}ms", times[0]);
            Console.WriteLine("Average:      {0}ms", (int)times.Average());
            Console.WriteLine("Median :      {0}ms", times[times.Count/2]);
            Console.WriteLine("90%    :      {0}ms", times[(int)(times.Count*0.9)]);
            Console.WriteLine("Maximum:      {0}ms", times[times.Count - 1]);
            Console.WriteLine("==========================================");
            Console.ReadLine();
        }

        public static async Task<List<long>> Run(int concurrent, int count, Func<string, Task> call, string server)
        {
            var resultTimes = new List<long>(count);
            var tasks = new List<Task<long>>(concurrent);

            int done = 0;

            //fill list with tasks
            while(done < concurrent && done < count)
            {
                tasks.Add(RunSingle(call, server));
                done++;
            }

            while(tasks.Count > 0)
            {
                var completed = await Task.WhenAny(tasks);
                tasks.Remove(completed);

                resultTimes.Add(await completed);

                if(done < count)
                {
                    tasks.Add(RunSingle(call, server));
                    done++;
                }
            }

            return resultTimes;
        }

        private static async Task<long> RunSingle(Func<string, Task> task, string server)
        {
            var st = new Stopwatch();
            st.Start();

            await task(server);

            st.Stop();
            return st.ElapsedMilliseconds;
        }

        private static readonly Random _random = new Random();

        public static int RandomId(int max)
        {
            lock(_random)
                return _random.Next(max);
        }

        public static Task<string> DoRequestAsync(string server)
        {
            string url = string.Format("{0}/measurement/async/{1}", server, RandomId(25000));
            return DoWebRequest(url);
        }

        public static Task<string> DoRequest(string server)
        {
            string url = string.Format("{0}/measurement/sync/{1}", server, RandomId(25000));
            return DoWebRequest(url);
        }

        public static async Task<string> DoWebRequest(string url)
        {
            var request = WebRequest.CreateHttp(url);
            request.Method = "GET";
            request.KeepAlive = true;
            request.Pipelined = true;
            using(var response = await request.GetResponseAsync() as HttpWebResponse)
            {
                if(response == null)
                    throw new Exception("Expected HTTP response");

                if(response.StatusCode != HttpStatusCode.OK)
                    throw new Exception("Server returned error");

                using(var responseStream = response.GetResponseStream())
                {
                    using(var memStream = new MemoryStream())
                    {
                        if(responseStream != null)
                            await responseStream.CopyToAsync(memStream);

                        return Encoding.UTF8.GetString(memStream.ToArray());
                    }
                }
            }
        }
    }
}