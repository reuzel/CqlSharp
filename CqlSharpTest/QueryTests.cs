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

using CqlSharp.Protocol;
using CqlSharp.Serialization;
using CqlSharp.Tracing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp.Test
{
    [TestClass]
    public class QueryTests
    {
        private const string ConnectionString = "server=localhost;throttle=256;ConnectionStrategy=PartitionAware;loggerfactory=debug;loglevel=query";

        [TestInitialize]
        public void Init()
        {
            const string createKsCql =
                @"CREATE KEYSPACE Test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = 'false';";
            const string createTableCql = @"create table Test.BasicFlow (id int primary key, value text, ignored text);";
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

                var executions = new Task<ICqlQueryResult>[insertCount];

                var options = new ParallelOptions { MaxDegreeOfParallelism = 4 };
                Parallel.For(0, insertCount, options, i =>
                {
                    // ReSharper disable AccessToDisposedClosure
                    var cmd = new CqlCommand(connection, insertCql, CqlConsistency.One);
                    // ReSharper restore AccessToDisposedClosure
                    cmd.Prepare();

                    var b = new BasicFlowData { Id = i, Data = "Hallo " + i, Ignored = "none" };
                    cmd.PartitionKey.Set(b);
                    cmd.Parameters.Set(b);

                    executions[i] = cmd.ExecuteNonQueryAsync();
                });

                await Task.WhenAll(executions);

                var presence = new bool[insertCount];

                var selectCmd = new CqlCommand(connection, retrieveCql, CqlConsistency.One) { EnableTracing = true };

                CqlDataReader<BasicFlowData> reader = await selectCmd.ExecuteReaderAsync<BasicFlowData>();
                while (await reader.ReadAsync())
                {
                    BasicFlowData row = reader.Current;
                    Assert.AreEqual("Hallo " + row.Id, row.Data);
                    Assert.IsNull(row.Ignored);
                    presence[row.Id] = true;
                }

                Assert.IsTrue(presence.All(p => p));

                Assert.IsTrue(reader.TracingId.HasValue, "Expected a tracing id");

                var tracer = new QueryTraceCommand(connection, reader.TracingId.Value);
                TracingSession session = await tracer.GetTraceSessionAsync(CancellationToken.None);

                Assert.IsNotNull(session);
            }
        }

        [TestMethod]
        public async Task BasicFlowOnSynchronizationContext()
        {
            SynchronizationContext.SetSynchronizationContext(new STASynchronizationContext());
            await BasicFlow();
        }


        [TestMethod]
        public void BasicFlowAdo()
        {
            //Assume
            const string insertCql = @"insert into Test.BasicFlow (id,value) values (?,?);";
            const string retrieveCql = @"select id,value,ignored from Test.BasicFlow;";

            const int insertCount = 1000;

            //Act

            using (IDbConnection connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                IDbCommand cmd = new CqlCommand(connection, insertCql,
                                                CqlConsistency.One);
                cmd.Parameters.Add(new CqlParameter("id", CqlType.Int));
                cmd.Parameters.Add(new CqlParameter("value",
                                                    CqlType.Text));

                for (int i = 0; i < insertCount; i++)
                {
                    cmd.Prepare();

                    ((IDbDataParameter)cmd.Parameters["id"]).Value = i;
                    ((IDbDataParameter)cmd.Parameters["value"]).Value =
                        "Hallo " + i;

                    cmd.ExecuteNonQuery();
                }

                var presence = new bool[insertCount];

                IDbCommand selectCmd = new CqlCommand(connection, retrieveCql, CqlConsistency.One) { EnableTracing = true };
                IDataReader reader = selectCmd.ExecuteReader();

                DataTable schema = reader.GetSchemaTable();
                Assert.AreEqual(3, schema.Rows.Count);
                Assert.IsTrue(schema.Rows.Cast<DataRow>().Any(row => row[CqlSchemaTableColumnNames.ColumnName].Equals("ignored")));

                while (reader.Read())
                {
                    int id = reader.GetInt32(0);
                    string value = reader.GetString(1);
                    Assert.IsTrue(reader.IsDBNull(2));
                    Assert.AreEqual("Hallo " + id, value);
                    presence[id] = true;
                }

                Assert.IsTrue(presence.All(p => p));
            }
        }
    }

    #region Nested type: BasicFlowData

    [CqlTable("basicflow", Keyspace = "test")]
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
