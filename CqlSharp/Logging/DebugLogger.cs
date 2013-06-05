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
using System.Diagnostics;

namespace CqlSharp.Logging
{
    /// <summary>
    ///   Logger factory for logging to Debug output window
    /// </summary>
    internal class DebugLoggerFactory : ILoggerFactory
    {
        /// <summary>
        ///   The debug logger instances
        /// </summary>
        private readonly ConcurrentDictionary<string, DebugLogger> _loggers =
            new ConcurrentDictionary<string, DebugLogger>();

        #region Implementation of ILoggerFactory

        /// <summary>
        ///   Gets the name for this logger implementation. E.g. Null, Debug, Log4Net
        /// </summary>
        /// <value> The name </value>
        public string Name
        {
            get { return "Debug"; }
        }

        /// <summary>
        ///   Creates a logger implementation.
        /// </summary>
        /// <param name="name"> The name. </param>
        /// <returns> </returns>
        public ILogger CreateLogger(string name)
        {
            return _loggers.GetOrAdd(name, n => new DebugLogger(n));
        }

        #endregion
    }

    /// <summary>
    ///   Logger that logs to debug output
    /// </summary>
    internal class DebugLogger : ILogger
    {
        private readonly string _name;

        public DebugLogger(string name)
        {
            _name = name;
        }

        #region Implementation of ILogger

        public void LogVerbose(Guid traceId, string format, params object[] values)
        {
            Debug.WriteLine("    DEBUG - {0} - {1} - {2} - {3}", _name, DateTime.Now, traceId,
                            string.Format(format, values));
        }

        public void LogQuery(Guid traceId, string format, params object[] values)
        {
            Debug.WriteLine("QUERY - {0} - {1} - {2} - {3}", _name, DateTime.Now, traceId, string.Format(format, values));
        }

        public void LogInfo(Guid traceId, string format, params object[] values)
        {
            Debug.WriteLine("INFO - {0} - {1} - {2} - {3}", _name, DateTime.Now, traceId, string.Format(format, values));
        }

        public void LogWarning(Guid traceId, string format, params object[] values)
        {
            Debug.WriteLine("WARNING - {0} - {1} - {2} - {3}", _name, DateTime.Now, traceId,
                            string.Format(format, values));
        }

        public void LogError(Guid traceId, string format, params object[] values)
        {
            Debug.WriteLine("ERROR - {0} - {1} - {2} - {3}", _name, DateTime.Now, traceId, string.Format(format, values));
        }

        public void LogCritical(Guid traceId, string format, params object[] values)
        {
            Debug.WriteLine("CRITICAL - {0} - {1} - {2} - {3}", _name, DateTime.Now, traceId,
                            string.Format(format, values));
        }

        #endregion
    }
}