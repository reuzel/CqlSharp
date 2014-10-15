// CqlSharp - CqlSharp.Test
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
using System.Diagnostics;
using System.Threading.Tasks;
using CqlSharp.Protocol;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Test
{
    [TestClass]
    public class CqlConnectionTest
    {
        [ClassCleanup]
        public static void Cleanup()
        {
            CqlConnection.ShutdownAll();
        }

        [TestMethod]
        public void AuthenticateError()
        {
            try
            {
                using(
                    var connection =
                        new CqlConnection(
                            "Servers=localhost;username=doesNotExist;password=too;loggerfactory=debug;loglevel=verbose")
                    )
                {
                    connection.Open();
                }
            }
            catch(AuthenticationException uex)
            {
                Debug.WriteLine("Expected Unauthenticated exception: {0}", uex);
            }
            catch(Exception ex)
            {
                Assert.Fail("Wrong exception thrown: {0}", ex.GetType().Name);
            }
        }

        [TestMethod]
        public void DefaultDatabaseSet()
        {
            //Act
            using(var connection = new CqlConnection("Servers=localhost;Database=test2"))
            {
                Assert.AreEqual("test2", connection.Database);
            }
        }

        [TestMethod]
        public void ConnectToUnknownDb()
        {
            try
            {
                //Act
                using(
                    var connection =
                        new CqlConnection(
                            "Servers=localhost;Database=DoesNotExist;username=cassandra;password=cassandra;loggerfactory=debug;loglevel=verbose")
                    )
                {
                    connection.Open();

                    //invoke random command, as it will try to change database on the connection
                    var command = new CqlCommand(connection, "select * from randomTable;");
                    var reader = command.ExecuteReader();
                    reader.Dispose();
                }
            }
            catch(Exception ex)
            {
                Assert.IsInstanceOfType(ex, typeof(InvalidException));
                return;
            }

            Assert.Fail("Exception should have been thrown;");
        }

        [TestMethod]
        [ExpectedException(typeof(CqlException))]
        public async Task ConnectTimeoutThrowsProperException()
        {
            using(
                var connection =
                    new CqlConnection(
                        "servers=192.168.100.100,192.168.100.101;SocketConnectTimeout=1000;Logger=Debug;LogLevel=Verbose")
                )
            {
                await connection.OpenAsync();
            }
        }

        [TestMethod]
        public void TestConnectionStringValueSerialization()
        {
            var builder =
                new CqlConnectionStringBuilder(
                    "servers=ip1,ip2;SocketKeepAlive=off;SocketSoLinger=10;SocketConnectTimeout=-10");

            Assert.AreEqual(-1, builder.SocketKeepAlive);
            Assert.AreEqual(10, builder.SocketSoLinger);
            Assert.AreEqual(-1, builder.SocketConnectTimeout);
        }

        [TestMethod]
        public void TrySetKeepAlive()
        {
            var builder =
                new CqlConnectionStringBuilder(
                    "node=localhost;Logger=Debug;LogLevel=Verbose;Username=cassandra;password=cassandra");
            builder.SocketKeepAlive = 10*60*1000; //10 mins

            using(var connection = new CqlConnection(builder))
            {
                connection.Open();
            }
        }

        [TestMethod]
        public async Task OpenAsync()
        {
            using(
                var connection =
                    new CqlConnection(
                        "servers=localhost;username=cassandra;password=cassandra;MaxConnectionIdleTime=1200;Logger=Debug;LogLevel=Verbose;DiscoveryScope=Cluster")
                )
            {
                await connection.OpenAsync();
            }
        }

        [TestMethod]
        public async Task OpenAsyncNoRetry()
        {
            using (
                var connection =
                    new CqlConnection(
                        "servers=localhost;username=cassandra;password=cassandra;MaxConnectionIdleTime=1200;Logger=Debug;LogLevel=Verbose;DiscoveryScope=Cluster;MaxQueryRetries=0;CommandTimeout=1")
                )
            {
                await connection.OpenAsync();
            }
        }
    }
}