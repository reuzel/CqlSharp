using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp.Performance.Client
{
    class Program
    {
        static void Main(string[] args)
        {
            var options = new Options();
            if(!CommandLine.Parser.Default.ParseArguments(args, options))
                return;

            Func<Task> request = options.Sync ? (Func<Task>)DoRequest : DoRequestAsync;

            Console.WriteLine("Waiting for server to start...");
            Thread.Sleep(5000);

            ThreadPool.SetMinThreads(options.Concurrent + 10, options.Concurrent + 10);
            
            Console.WriteLine("Sending warmup request");
            DoRequest().Wait();
            Thread.Sleep(5000);

            Console.WriteLine("Starting...");

            var st = new Stopwatch();
            st.Start();

            var times = Run(options.Concurrent, options.Requests, request).Result;

            st.Stop();
            times.Sort();

            Console.WriteLine();
            Console.WriteLine("== [OVERALL] =============================");
            Console.WriteLine("API:          {0}", options.Sync ? "Sync" : "Async");
            Console.WriteLine("Concurrent:   {0}", options.Concurrent);
            Console.WriteLine("Total Time:   {0}ms", st.ElapsedMilliseconds);
            Console.WriteLine("Requests:     {0}", options.Requests);
            Console.WriteLine("Requests/sec: {0}", (int)((double)options.Requests/ st.ElapsedMilliseconds * 1000));
            Console.WriteLine("== [TIMING] ==============================");
            Console.WriteLine("Minimum:      {0}ms", times[0]);
            Console.WriteLine("Average:      {0}ms", (int)times.Average());
            Console.WriteLine("Median :      {0}ms", times[times.Count/2]);
            Console.WriteLine("90%    :      {0}ms", times[(int)(times.Count*0.9)]);
            Console.WriteLine("Maximum:      {0}ms", times[times.Count-1]);
            Console.WriteLine("==========================================");
            Console.ReadLine();
        }
        
        public static async Task<List<long>> Run(int concurrent, int count, Func<Task> call)
        {
            var resultTimes = new List<long>(count);
            var tasks = new List<Task<long>>(concurrent);

            int done = 0;

            //fill list with tasks
            while(done < concurrent && done < count)
            {
                tasks.Add(RunSingle(call));
                done++;
            }
            
            while(tasks.Count>0)
            {
                var completed = await Task.WhenAny(tasks);
                tasks.Remove(completed);

                resultTimes.Add(await completed);

                if(done < count)
                {
                   
                    tasks.Add(RunSingle(call));
                    done++;
                }
            }

            return resultTimes;
        }

        private static async Task<long> RunSingle(Func<Task> task)
        {
            var st = new Stopwatch();
            st.Start();

            await task();

            st.Stop();
            return st.ElapsedMilliseconds;
        }

        private static readonly Random _random = new Random();
        public static int RandomId(int max)
        {
            lock(_random)
               return  _random.Next(max);
        }

        public static Task<string> DoRequestAsync()
        {
            string url = "http://localhost/measurement/async/" + RandomId(25000);
            return DoWebRequest(url);
        }
        public static Task<string> DoRequest()
        {
            string url = "http://localhost/measurement/sync/" + RandomId(25000);
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
