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

using CqlSharp.Authentication;
using CqlSharp.Logging;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.ComponentModel.Composition.Registration;
using System.IO;
using System.Reflection;

namespace CqlSharp.Extensions
{
    internal class Loader
    {
        private static volatile Loader _loader;
        private static readonly object SyncLock = new object();

        private Loader()
        {
            LoadFactories();
        }

        public static Loader Extensions
        {
            get
            {
                if (_loader == null)
                {
                    lock (SyncLock)
                    {
                        if (_loader == null)
                        {
                            _loader = new Loader();
                        }
                    }
                }
                return _loader;
            }
        }

        public List<ILoggerFactory> LoggerFactories { get; set; }

        public List<IAuthenticatorFactory> AuthenticationFactories { get; set; }

        /// <summary>
        ///   Loads the logger factories.
        /// </summary>
        private void LoadFactories()
        {
            var conventions = new RegistrationBuilder();

            //search for ILoggerFactory implementations
            conventions
                .ForTypesDerivedFrom<ILoggerFactory>()
                .Export<ILoggerFactory>();

            //search for IAuthenticatorFactory implementations
            conventions
                .ForTypesDerivedFrom<IAuthenticatorFactory>()
                .Export<IAuthenticatorFactory>();

            //import into LoggerFactories
            conventions
                .ForType<Loader>()
                .ImportProperty(extensions => extensions.LoggerFactories);

            //import into AuthenticationFactories
            conventions
                .ForType<Loader>()
                .ImportProperty(extensions => extensions.AuthenticationFactories);

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
                //in case of any loading errors, load only the loggers and authenticators that we implement
                LoggerFactories = new List<ILoggerFactory>
                                      {
                                          new NullLoggerFactory(),
                                          new ConsoleLoggerFactory(),
                                          new DebugLoggerFactory()
                                      };

                AuthenticationFactories = new List<IAuthenticatorFactory>
                                              {
                                                  new PasswordAuthenticatorFactory()
                                              };
            }
        }
    }
}