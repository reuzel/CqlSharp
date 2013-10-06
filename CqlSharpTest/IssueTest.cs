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

using Microsoft.QualityTools.Testing.Fakes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Fakes;
using System.Threading.Tasks;

namespace CqlSharp.Test
{
    [TestClass]
    public class IssueTest
    {
        private const string ConnectionString =
            "server=localhost;loggerfactory=debug;loglevel=verbose;username=cassandra;password=cassandra";

        [ClassCleanup]
        public static void Cleanup()
        {
            CqlConnection.ShutdownAll();
        }

        [TestMethod]
        public async Task Issue15()
        {
            using (ShimsContext.Create())
            {
                //Assume
                //make DateTime.Now return a value in a different timezone
                ShimDateTime.NowGet = () =>
                                          {
                                              var timezone = TimeZoneInfo.CreateCustomTimeZone("Issue15Zone",
                                                                                               TimeSpan.FromHours(-5),
                                                                                               "Issue 15 zone",
                                                                                               "Issue 15 zone");

                                              return TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, timezone);
                                          };

                //Act
                using (var connection = new CqlConnection(ConnectionString))
                {
                    await connection.OpenAsync();
                }
            }
        }
    }
}