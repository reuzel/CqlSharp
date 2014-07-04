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
using System.Collections.Concurrent;

namespace CqlSharp.Logging
{
    public class LoggerFactory<T> : ILoggerFactory where T : ILogger
    {
        private readonly ConcurrentDictionary<string, T> _loggers =
            new ConcurrentDictionary<string, T>();

        /// <summary>
        /// Creates new instance of LoggerFactory with given name.
        /// </summary>
        /// <param name="name"> The name. </param>
        public LoggerFactory(string name)
        {
            Name = name;
        }

        #region Implementation of ILoggerFactory

        /// <summary>
        /// Gets the name for this logger implementation. E.g. Null, Console, Log4Net
        /// </summary>
        /// <value> The name </value>
        public string Name { get; protected set; }

        /// <summary>
        /// Creates a logger implementation.
        /// </summary>
        /// <param name="name"> The name. </param>
        /// <returns> </returns>
        public virtual ILogger CreateLogger(string name)
        {
            return _loggers.GetOrAdd(name, n => (T)Activator.CreateInstance(typeof(T), name));
                // not the fastest, but we cache the instance - so who cares
        }

        #endregion
    }
}