// CqlSharp - CqlSharpTest
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

using CqlSharp;
using CqlSharp.Protocol;
using CqlSharp.Serialization;
using CqlSharp.Tracing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;
using System.Threading.Tasks;

namespace CqlSharpTest
{
    [TestClass]
    public class QueryTests
    {
        private const string ConnectionString = "server=localhost;throttle=100;ConnectionStrategy=PartitionAware";

        [TestInitialize]
        public void Init()
        {
            const string createKsCql =
                @"CREATE KEYSPACE Test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = 'false';";
            const string createTableCql = @"create table Test.BasicFlow (id int primary key, value text);";
            const string truncateTableCql = @"truncate Test.BasicFlow;";

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                try
                {
                    var createKs = new CqlCommand(connection, createKsCql);
                    createKs.ExecuteNonQuery();
                }
                catch (AlreadyExistsException)
                {
                    //ignore
                }

                try
                {
                    var createTable = new CqlCommand(connection, createTableCql);
                    createTable.ExecuteNonQuery();
                }
                catch (AlreadyExistsException)
                {
                    var truncTable = new CqlCommand(connection, truncateTableCql);
                    truncTable.ExecuteNonQuery();
                }
            }
        }

        [TestCleanup]
        public void Cleanup()
        {
            const string dropCql = @"drop keyspace Test;";

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                try
                {
                    var drop = new CqlCommand(connection, dropCql);
                    drop.ExecuteNonQuery();
                }
                catch (InvalidException)
                {
                    //ignore
                }
            }
        }

        [TestMethod]
        public async Task BasicFlow()
        {
            //Assume
            const string insertCql = @"insert into Test.BasicFlow (id,value) values (?,?);";
            const string retrieveCql = @"select * from Test.BasicFlow;";

            const int insertCount = 1000;

            //Act
            using (var connection = new CqlConnection(ConnectionString))
            {
                await connection.OpenAsync();

                var cmd = new CqlCommand(connection, insertCql, CqlConsistency.One);
                await cmd.PrepareAsync();

                var executions = new Task<ICqlQueryResult>[insertCount];

                for (int i = 0; i < insertCount; i++)
                {
                    var b = new BasicFlowData { Id = i, Data = "Hallo " + i };
                    cmd.PartitionKey.Set(b);
                    cmd.Parameters.Set(b);

                    executions[i] = cmd.ExecuteNonQueryAsync();
                }

                await Task.WhenAll(executions);

                var presence = new bool[insertCount];

                var selectCmd = new CqlCommand(connection, retrieveCql, CqlConsistency.One) { EnableTracing = true };

                CqlDataReader<BasicFlowData> reader = await selectCmd.ExecuteReaderAsync<BasicFlowData>();
                while (await reader.ReadAsync())
                {
                    BasicFlowData row = reader.Current;
                    Assert.AreEqual(row.Data, "Hallo " + row.Id);
                    presence[row.Id] = true;
                }

                Assert.IsTrue(presence.All(p => p));

                var tracer = new QueryTraceCommand(connection, reader.TracingId.Value);
                TracingSession session = await tracer.GetTraceSessionAsync();

                Assert.IsNotNull(session);
            }
        }

        #region Nested type: BasicFlowData

        [CqlTable("BasicFlow", Keyspace = "Test")]
        public class BasicFlowData
        {
            [CqlColumn("id", PartitionKeyIndex = 0, CqlType = CqlType.Int)]
            public int Id;

            [CqlColumn("value")]
            public string Data { get; set; }

            [CqlIgnore]
            public string Ignored { get; set; }
        }

        #endregion
    }
}