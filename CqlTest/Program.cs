using System;
using System.Diagnostics;
using CqlSharpTest;

namespace CqlTest
{
    /// <summary>
    /// Utility program that executes a set of tests. Usefull for profiling purposes...
    /// </summary>
    internal class Program
    {
        public static void Main(string[] args)
        {
            var test = new QueryTests();

            test.Init();

            var st = new Stopwatch();
            st.Start();

            test.BasicFlow().Wait();

            st.Stop();

            test.Cleanup();

            Console.WriteLine("Done in {0}", st.Elapsed);
        }
    }
}