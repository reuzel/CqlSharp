using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Performance.Client
{
    class Program
    {
        static void Main(string[] args)
        {
            using(var client = new HttpClient())
            {
                client.BaseAddress = new Uri("http://localhost/");

                var response = client.GetAsync("api/measurement").Result;
                Console.WriteLine(response.Content.ReadAsStringAsync().Result);
                Console.ReadLine();
            }
        }
    }
}
