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

using CqlSharp.Extensions;
using System;
using System.Linq;

namespace CqlSharp.Logging
{
    internal class LoggerManager
    {
        private readonly string _factoryName;
        private readonly LogLevel _level;
        private readonly object _syncLock = new object();

        private volatile ILoggerFactory _factory;

        public LoggerManager(string loggerFactoryName, LogLevel level)
        {
            _factoryName = loggerFactoryName;
            _level = level;
        }

        /// <summary>
        ///   Gets a logger instance with the specified name.
        /// </summary>
        /// <param name="name"> The name. </param>
        /// <returns> </returns>
        public Logger GetLogger(string name)
        {
            if (_factory == null)
            {
                lock (_syncLock)
                {
                    if (_factory == null)
                    {
                        var factory =
                            Loader.Extensions.LoggerFactories.FirstOrDefault(
                                f => f.Name.Trim().Equals(_factoryName, StringComparison.InvariantCultureIgnoreCase));

                        _factory = factory ?? new NullLoggerFactory();
                    }
                }
            }

            var loggerImpl = _factory.CreateLogger(name);
            return new Logger(loggerImpl, _level);
        }
    }
}