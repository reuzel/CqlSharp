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

namespace CqlSharp.Logging
{
    /// <summary>
    ///   Logger factory for logging to Console output window
    /// </summary>
    internal class ConsoleLoggerFactory : ILoggerFactory
    {
        /// <summary>
        ///   The Console logger instances
        /// </summary>
        private readonly ConcurrentDictionary<string, ConsoleLogger> _loggers =
            new ConcurrentDictionary<string, ConsoleLogger>();

        #region Implementation of ILoggerFactory

        /// <summary>
        ///   Gets the name for this logger implementation. E.g. Null, Console, Log4Net
        /// </summary>
        /// <value> The name </value>
        public string Name
        {
            get { return "Console"; }
        }

        /// <summary>
        ///   Creates a logger implementation.
        /// </summary>
        /// <param name="name"> The name. </param>
        /// <returns> </returns>
        public ILogger CreateLogger(string name)
        {
            return _loggers.GetOrAdd(name, n => new ConsoleLogger(n));
        }

        #endregion
    }

    /// <summary>
    ///   Logger that logs to Console output
    /// </summary>
    internal class ConsoleLogger : ILogger
    {
        private static readonly object SyncLock = new object();

        private readonly string _name;

        public ConsoleLogger(string name)
        {
            _name = name;
        }

        #region Implementation of ILogger

        public void LogVerbose(Guid traceId, string format, params object[] values)
        {
            lock (SyncLock)
            {
                Console.ForegroundColor = ConsoleColor.DarkGray;
                Console.WriteLine("    DEBUG - {0} - {1} - {2} - {3}", _name, DateTime.Now, traceId,
                                  string.Format(format, values));
                Console.ResetColor();
            }
        }

        public void LogQuery(Guid traceId, string format, params object[] values)
        {
            lock (SyncLock)
            {
                Console.ForegroundColor = ConsoleColor.DarkGreen;
                Console.WriteLine("QUERY - {0} - {1} - {2} - {3}", _name, DateTime.Now, traceId,
                                  string.Format(format, values));
                Console.ResetColor();
            }
        }

        public void LogInfo(Guid traceId, string format, params object[] values)
        {
            lock (SyncLock)
            {
                Console.WriteLine("INFO - {0} - {1} - {2} - {3}", _name, DateTime.Now, traceId,
                                  string.Format(format, values));
            }
        }

        public void LogWarning(Guid traceId, string format, params object[] values)
        {
            lock (SyncLock)
            {
                Console.ForegroundColor = ConsoleColor.DarkMagenta;
                Console.WriteLine("WARNING - {0} - {1} - {2} - {3}", _name, DateTime.Now, traceId,
                                  string.Format(format, values));
                Console.ResetColor();
            }
        }

        public void LogError(Guid traceId, string format, params object[] values)
        {
            lock (SyncLock)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR - {0} - {1} - {2} - {3}", _name, DateTime.Now, traceId,
                                  string.Format(format, values));
                Console.ResetColor();
            }
        }

        public void LogCritical(Guid traceId, string format, params object[] values)
        {
            lock (SyncLock)
            {
                Console.ForegroundColor = ConsoleColor.DarkRed;
                Console.WriteLine("CRITICAL - {0} - {1} - {2} - {3}", _name, DateTime.Now, traceId,
                                  string.Format(format, values));
                Console.ResetColor();
            }
        }

        #endregion
    }
}