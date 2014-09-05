// CqlSharp - CqlSharp.Performance.Web
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

using System.Diagnostics;
using System.Threading;
using System.Web.Http;
using CqlSharp.Performance.Data;
using NLog;

namespace CqlSharp.Performance.Web.Controllers
{
    /// <summary>
    /// Allows retrieval of measurements synchronous
    /// </summary>
    public class SyncController : ApiController
    {
        private static readonly Logger Log = LogManager.GetLogger("Web.CqlSharp.Sync");

        private static int _par;

        public Measurement Get(int id)
        {
            Interlocked.Increment(ref _par);
            var st = new Stopwatch();
            st.Start();

            var m = MeasurementManager.GetMeasurement(id);

            st.Stop();
            var par = Interlocked.Decrement(ref _par);

            Log.Trace("Parallel: {0}, Execution time: {1}ms", (object)par, (object)st.ElapsedMilliseconds);

            return m;
        }
    }
}