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
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.ComponentModel.Composition.Registration;
using System.IO;
using System.Linq;
using System.Reflection;

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

        public IEnumerable<ILoggerFactory> LoggerFactories { get; set; }

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
                        LoadLoggerFactories();

                        var factory =
                            LoggerFactories.FirstOrDefault(
                                f => f.Name.Trim().Equals(_factoryName, StringComparison.InvariantCultureIgnoreCase));

                        _factory = factory ?? new NullLoggerFactory();
                    }
                }
            }

            var loggerImpl = _factory.CreateLogger(name);
            return new Logger(loggerImpl, _level);
        }

        /// <summary>
        ///   Loads the logger factories.
        /// </summary>
        public void LoadLoggerFactories()
        {
            var conventions = new RegistrationBuilder();

            //search for ILoggerFactory implementations
            conventions
                .ForTypesDerivedFrom<ILoggerFactory>()
                .Export<ILoggerFactory>();

            //inport into LoggerFactories
            conventions
                .ForType<LoggerManager>()
                .ImportProperty(manager => manager.LoggerFactories);

            //create catalog of dlls
            var catalog = new AggregateCatalog();

            //go and search for ILoggerFactories in executing directory
            string path = Directory.GetParent(Assembly.GetExecutingAssembly().Location).ToString();
            catalog.Catalogs.Add(new DirectoryCatalog(path, conventions));

            //or in bin directory (asp.net)
            if (Directory.Exists(path + "\\bin"))
                catalog.Catalogs.Add(new DirectoryCatalog(path + "\\bin", conventions));

            //create container
            var container = new CompositionContainer(catalog, CompositionOptions.Default);

            try
            {
                //get me my factories 
                container.SatisfyImportsOnce(this, conventions);
            }
            catch
            {
                //in case of any loading errors, assume no factories are loaded
                LoggerFactories = new List<ILoggerFactory>();
            }
        }
    }
}