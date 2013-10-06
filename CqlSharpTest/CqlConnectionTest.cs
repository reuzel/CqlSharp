// CqlSharp - CqlSharp.Test
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

using CqlSharp.Protocol;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Diagnostics;

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
                using (var connection = new CqlConnection("Servers=localhost;username=doesNotExist;password=too;loggerfactory=debug;loglevel=verbose"))
                {
                    connection.Open();
                }
            }
            catch (AuthenticationException uex)
            {
                Debug.WriteLine("Expected Unauthenticated exception: {0}", uex);
            }
            catch (Exception ex)
            {
                Assert.Fail("Wong exception thrown: {0}", ex.GetType().Name);
            }
        }

        [TestMethod]
        public void DefaultDatabaseSet()
        {
            //Act
            using (var connection = new CqlConnection("Servers=localhost;Database=test2"))
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
                using (var connection = new CqlConnection("Servers=localhost;Database=DoesNotExist;username=cassandra;password=cassandra;loggerfactory=debug;loglevel=verbose"))
                {
                    connection.Open();

                    //invoke random command, as it will try to change database on the connection
                    var command = new CqlCommand(connection, "select * from randomTable;");
                    var reader = command.ExecuteReader();
                    reader.Dispose();

                }
            }
            catch (Exception ex)
            {
                Assert.IsInstanceOfType(ex, typeof(InvalidException));
                return;
            }

            Assert.Fail("Exception should have been thrown;");
        }
    }
}