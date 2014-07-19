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
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.ComponentModel.Composition.Registration;
using System.IO;
using CqlSharp.Authentication;
using CqlSharp.Logging;
using CqlSharp.Serialization.Marshal;

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
                if(_loader == null)
                {
                    lock(SyncLock)
                    {
                        if(_loader == null)
                            _loader = new Loader();
                        }
                    }
                return _loader;
            }
        }

        public List<ILoggerFactory> LoggerFactories { get; set; }

        public List<IAuthenticatorFactory> AuthenticationFactories { get; set; }

        public List<ITypeFactory> TypeFactories { get; set; }

        /// <summary>
        /// Loads the logger factories.
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

            //search for ISerializerFactory implementations
            conventions
                .ForTypesDerivedFrom<ITypeFactory>()
                .Export<ITypeFactory>();

            //import into LoggerFactories
            conventions
                .ForType<Loader>()
                .ImportProperty(extensions => extensions.LoggerFactories);

            //import into AuthenticationFactories
            conventions
                .ForType<Loader>()
                .ImportProperty(extensions => extensions.AuthenticationFactories);

            //import into AuthenticationFactories
            conventions
                .ForType<Loader>()
                .ImportProperty(extensions => extensions.TypeFactories);

            //create catalog of dlls
            var catalog = new AggregateCatalog();

            //go and search for ILoggerFactories in executing directory
            string path = AppDomain.CurrentDomain.BaseDirectory;
            catalog.Catalogs.Add(new DirectoryCatalog(path, conventions));

            string subPath = AppDomain.CurrentDomain.RelativeSearchPath;

            //or in bin directory (asp.net)
            if (!string.IsNullOrEmpty(subPath) && Directory.Exists(subPath))
                catalog.Catalogs.Add(new DirectoryCatalog(subPath, conventions));

            //create container
            var container = new CompositionContainer(catalog, CompositionOptions.Default);

            try
            {
                //get me my factories 
                container.SatisfyImportsOnce(this, conventions);
            }
            catch
            {
                //in case of any loading errors, load only the loggers and authenticators, and serializers that we implement
                LoggerFactories = new List<ILoggerFactory>
                                      {
                                          new NullLoggerFactory(),
                                          new ConsoleLoggerFactory(),
                                          new DebugLoggerFactory(),
                                          new TraceLoggerFactory()
                                      };

                AuthenticationFactories = new List<IAuthenticatorFactory>
                                              {
                                                  new PasswordAuthenticatorFactory()
                                              };

                TypeFactories = new List<ITypeFactory>
                {
                    new AsciiTypeFactory(),
                    new BooleanTypeFactory(),
                    new BytesTypeFactory(),
                    new CounterColumnTypeFactory(),
                    new DateTypeFactory(),
                    new DecimalTypeFactory(),
                    new DoubleTypeFactory(),
                    new FloatTypeFactory(),
                    new InetAddressTypeFactory(),
                    new Int32TypeFactory(),
                    new IntegerTypeFactory(),
                    new LexicalUUIDTypeFactory(),
                    new ListTypeFactory(),
                    new LongTypeFactory(),
                    new MapTypeFactory(),
                    new SetTypeFactory(),
                    new TimestampTypeFactory(),
                    new TimeUUIDTypeFactory(),
                    new UTF8TypeFactory(),
                    new UUIDTypeFactory()
                };
            }
        }
    }
}