// CqlSharp.Linq - CqlSharp.Linq.Test
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

using System.Collections;
using CqlSharp.Linq.Mutations;
using CqlSharp.Linq.Query;
using CqlSharp.Protocol;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace CqlSharp.Linq.Test
{
    [TestClass]
    public class DatabaseTest
    {
        private const string ConnectionString =
            "server=localhost;throttle=256;MaxConnectionIdleTime=3600;loggerfactory=debug;loglevel=Verbose;username=cassandra;password=cassandra;database=linqtest";

        [ClassInitialize]
        public static void Init(TestContext context)
        {
            const string createConnection = "Server=localhost;username=cassandra;password=cassandra";

            const string createKsCql =
                @"CREATE KEYSPACE LinqTest WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = 'false';";
            const string createTableCql =
                @"create table linqtest.myvalue (id int primary key, value text, ignored text);";


            using (var connection = new CqlConnection(createConnection))
            {
                connection.SetConnectionTimeout(0);
                connection.Open();

                try
                {
                    var createKs = new CqlCommand(connection, createKsCql);
                    createKs.ExecuteNonQuery();

                    var createTable = new CqlCommand(connection, createTableCql);
                    createTable.ExecuteNonQuery();

                    using (var transaction = connection.BeginTransaction())
                    {
                        transaction.BatchType = CqlBatchType.Unlogged;

                        var insert = new CqlCommand(connection, "insert into linqtest.myvalue (id,value) values(?,?)");
                        insert.Transaction = transaction;
                        insert.Prepare();

                        for (int i = 0; i < 10000; i++)
                        {
                            insert.Parameters[0].Value = i;
                            insert.Parameters[1].Value = "Hallo " + i;
                            insert.ExecuteNonQuery();
                        }

                        transaction.Commit();
                    }
                }
                catch (AlreadyExistsException)
                {
                    //ignore
                }
            }

            CqlConnection.Shutdown(createConnection);
        }

        [ClassCleanup]
        public static void Cleanup()
        {
            const string dropCql = @"drop keyspace linqtest;";

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

            CqlConnection.ShutdownAll();
        }

        [TestMethod]
        public void WhereThenSelect()
        {
            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Where(v => v.Id == 1).Select(v => v.Value).First();
                Assert.AreEqual("Hallo 1", value);
            }
        }


        [TestMethod]
        public void WhereThenSelectEnumerable()
        {
            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Where(v => v.Id == 1).Select(v => v.Value);
                foreach(var val in (IEnumerable)value)
                {
                    Assert.AreEqual("Hallo 1", val);
                }
            }
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidException))]
        public void WhereUsingWrongKey()
        {
            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Where(v => v.Value == "Hallo 1").Select(v => v.Id).First();
            }
        }

        [TestMethod]
        public void CountFourItems()
        {
            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Count(r => new[] { 1, 2, 3, 4 }.Contains(r.Id));

                Assert.AreEqual(4, value, "Unexpected number of results");
            }
        }

        [TestMethod]
        public void WhereContains()
        {
            using (var context = new MyContext(ConnectionString))
            {
                var values = context.Values.Where(r => new[] { 1, 2, 3, 4 }.Contains(r.Id)).ToList();

                Assert.AreEqual(4, values.Count, "Unexpected number of results");
                for (int i = 1; i <= 4; i++)
                {
                    Assert.IsTrue(values.Any(v => v.Id == i), "Missing value " + i);
                }
            }
        }

        [TestMethod]
        public void WhereContainsToDictionary()
        {
            using (var context = new MyContext(ConnectionString))
            {
                var values = context.Values.Where(r => new[] { 1, 2, 3, 4 }.Contains(r.Id)).ToDictionary(v => v.Id);

                Assert.AreEqual(4, values.Count, "Unexpected number of results");
                for (int i = 1; i <= 4; i++)
                {
                    Assert.IsTrue(values.Keys.Any(key => key == i), "Missing value " + i);
                }
            }
        }

        [TestMethod]
        public void WhereContainsWithConsistency()
        {
            using (var context = new MyContext(ConnectionString))
            {
                var values = context.Values.Where(r => new[] { 1, 2, 3, 4 }.Contains(r.Id)).WithConsistency(CqlConsistency.One).ToList();

                Assert.AreEqual(4, values.Count, "Unexpected number of results");
                for (int i = 1; i <= 4; i++)
                {
                    Assert.IsTrue(values.Any(v => v.Id == i), "Missing value " + i);
                }
            }
        }

        [TestMethod]
        public void WhereContainsWithPageSize()
        {
            using (var context = new MyContext(ConnectionString))
            {
                if (context.Database.Connection.State == ConnectionState.Closed)
                    context.Database.Connection.Open();

                if (context.Database.Connection.ServerVersion.CompareTo("2.0.4") < 0)
                {
                    Debug.WriteLine("Cassandra bug in place: https://issues.apache.org/jira/browse/CASSANDRA-6464");
                    return;
                }

                var values = context.Values.Where(r => new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21 }.Contains(r.Id)).WithPageSize(10).ToList();

                Assert.AreEqual(21, values.Count, "Unexpected number of results");
                for (int i = 1; i <= 21; i++)
                {
                    Assert.IsTrue(values.Any(v => v.Id == i), "Missing value " + i);
                }
            }
        }

        [TestMethod]
        public void EmumerateMultiple()
        {
            using (var context = new MyContext(ConnectionString))
            {
                var query = context.Values.Where(r => new[] { 1, 2, 3, 4 }.Contains(r.Id));

                Assert.AreEqual(4, query.Count(), "Unexpected number of results");
                var results = query.ToList();
                for (int i = 1; i <= 4; i++)
                {
                    Assert.IsTrue(results.Any(v => v.Id == i), "Missing value " + i);
                }
            }
        }

        [TestMethod]
        public void FindAndUpdate()
        {
            using (var context = new MyContext(ConnectionString))
            {
                context.Database.Log = cql => Debug.WriteLine("EXECUTE QUERY: " + cql);
                var value = context.Values.Find(100);
                value.Value = "Hallo daar!";
                context.SaveChanges();
            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(100);
                Assert.IsNotNull(value);
                Assert.AreEqual("Hallo daar!", value.Value);
            }
        }

        [TestMethod]
        public void FindUpdateAndReload()
        {
            using (var context = new MyContext(ConnectionString))
            {
                context.Database.Log = cql => Debug.WriteLine("EXECUTE QUERY: " + cql);

                //get entity
                var value = context.Values.Find(101);
                Assert.IsNotNull(value);

                //get entry
                var entry = context.ChangeTracker.Entry(value);
                Assert.IsNotNull(entry);

                //change it
                value.Value = "Hallo daar!";
                Assert.IsTrue(context.ChangeTracker.DetectChanges());
                Assert.AreEqual(EntityState.Modified, entry.State);

                //reload
                entry.Reload();
                Assert.AreEqual(EntityState.Unchanged, entry.State);
                Assert.IsFalse(context.ChangeTracker.HasChanges());
            }
        }

        [TestMethod]
        public void FindAndFindAgain()
        {
            int count = 0;

            using (var context = new MyContext(ConnectionString))
            {
                context.Database.Log = cql =>
                {
                    count++;
                    Debug.WriteLine("EXECUTE QUERY: " + cql);
                };

                var value = context.Values.Find(100);
                var value2 = context.Values.Find(100);

                //check if same value is returned
                Assert.AreSame(value, value2);

                //only one query was executed
                Assert.AreEqual(1, count);
            }
        }

        [TestMethod]
        public void UpdateTwiceInSingleTransaction()
        {
            using (var context = new MyContext(ConnectionString))
            using (var transaction = context.Database.BeginTransaction())
            {
                context.Database.Log = cql => Debug.WriteLine("EXECUTE QUERY: " + cql);

                var value = context.Values.Find(200);
                value.Value = "Hallo daar!";
                context.SaveChanges();

                value.Value = "Oops...";
                context.SaveChanges();

                transaction.Commit();
            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(200);
                Assert.IsNotNull(value);
                Assert.AreEqual("Oops...", value.Value);
            }
        }

        [TestMethod]
        public void UpdateTwiceInSingleTransactionAndRollback()
        {
            using (var context = new MyContext(ConnectionString))
            using (var transaction = context.Database.BeginTransaction())
            {
                context.Database.Log = cql => Debug.WriteLine("EXECUTE QUERY: " + cql);

                var value = context.Values.Find(300);
                value.Value = "Hallo daar!";
                context.SaveChanges();

                value.Value = "Oops...";
                context.SaveChanges();

                transaction.Rollback();
            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(300);
                Assert.IsNotNull(value);
                Assert.AreEqual("Hallo 300", value.Value);
            }
        }

        [TestMethod]
        public void UpdateTwiceInTwoTransactions()
        {
            using (var context = new MyContext(ConnectionString))
            {
                context.Database.Log = cql => Debug.WriteLine("EXECUTE QUERY: " + cql);

                var value = context.Values.Find(400);

                using (var transaction1 = context.Database.BeginTransaction())
                {
                    value.Value = "Hallo daar!";
                    context.SaveChanges();
                    transaction1.Commit();
                }

                using (var transaction2 = context.Database.BeginTransaction())
                {
                    transaction2.BatchType = CqlBatchType.Unlogged;

                    value.Value = "Nog een keer";
                    context.SaveChanges();
                    transaction2.Commit();
                }
            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(400);
                Assert.IsNotNull(value);
                Assert.AreEqual("Nog een keer", value.Value);
            }
        }

        [TestMethod]
        public void UpdateInExternalTransaction()
        {
            using (var connection = new CqlConnection(ConnectionString))
            using (var transaction = connection.BeginTransaction())
            using (var context = new MyContext(connection, false))
            {
                context.Database.Log = cql => Debug.WriteLine("EXECUTE QUERY: " + cql);

                context.Database.UseTransaction(transaction);

                var value = context.Values.Find(500);
                value.Value = "Hallo daar!";
                context.SaveChanges(false);

                var command = new CqlCommand(connection, "update myvalue set value='adjusted' where id=500");
                command.Transaction = transaction;
                command.ExecuteNonQuery();

                transaction.Commit();

                //accept all changes only after commit
                context.AcceptAllChanges();
            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(500);
                Assert.IsNotNull(value);
                Assert.AreEqual("adjusted", value.Value);
            }
        }

        [TestMethod]
        public void SelectAndUpdate()
        {
            using (var context = new MyContext(ConnectionString))
            {
                context.Database.Log = cql => Debug.WriteLine("EXECUTE QUERY: " + cql);
                var query = context.Values.Where(r => new[] { 201, 202, 203, 204 }.Contains(r.Id)).ToList();
                query[1].Value = "Zo gaan we weer verder";
                context.SaveChanges();
            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(202);
                Assert.IsNotNull(value);
                Assert.AreEqual("Zo gaan we weer verder", value.Value);
            }
        }

        [TestMethod]
        public async Task SelectAndDelete()
        {
            using (var context = new MyContext(ConnectionString))
            {
                context.Database.Log = cql => Debug.WriteLine("EXECUTE QUERY: " + cql);
                var query = context.Values.Where(r => new[] { 701, 702, 703, 704 }.Contains(r.Id)).ToList();

                EntityEntry<MyValue> entry = context.ChangeTracker.Entry(query[1]);
                Assert.IsNotNull(entry);

                Assert.AreEqual(EntityState.Unchanged, entry.State);

                Assert.IsTrue(context.Values.Delete(query[1]));

                Assert.AreEqual(EntityState.Deleted, entry.State);

                await context.SaveChangesAsync();

                Assert.AreEqual(EntityState.Detached, entry.State);

            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(702);
                Assert.IsNull(value, "Value was not deleted");
            }
        }

        [TestMethod]
        public void AddNewEntity()
        {
            int count = 0;
            using (var context = new MyContext(ConnectionString))
            {
                context.Database.Log = cql =>
                {
                    count++;
                    Debug.WriteLine("EXECUTE QUERY: " + cql);
                };

                var newValue = new MyValue { Id = 20000, Value = "Hallo 20000" };
                bool added = context.Values.Add(newValue);
                Assert.IsTrue(added);
                context.SaveChanges();

                //try save again (should do nothing)
                context.SaveChanges();
                Assert.AreEqual(1, count, "Save again introduces new query!");

                //try find (should do nothing)
                var entity = context.Values.Find(20000);
                Assert.AreSame(newValue, entity);
                Assert.AreEqual(1, count, "Find introduces new query!");
            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(20000);
                Assert.IsNotNull(value);
                Assert.AreEqual("Hallo 20000", value.Value);
            }
        }

        [TestMethod]
        public void AddAndChangeNewEntity()
        {
            int count = 0;
            using (var context = new MyContext(ConnectionString))
            {
                context.Database.Log = cql =>
                {
                    count++;
                    Debug.WriteLine("EXECUTE QUERY: " + cql);
                };

                var newValue = new MyValue { Id = 30000, Value = "Hallo 30000" };
                bool added = context.Values.Add(newValue);
                Assert.IsTrue(added);


                IEntityEntry entry = context.ChangeTracker.Entry(newValue);
                Assert.IsNotNull(entry);
                Assert.AreEqual(EntityState.Added, entry.State);

                context.SaveChanges();

                Assert.AreEqual(EntityState.Unchanged, entry.State);

                newValue.Value = "Hallo weer!";
                context.SaveChanges();
                Assert.AreEqual(2, count, "Where is my query?");
            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(30000);
                Assert.IsNotNull(value);
                Assert.AreEqual("Hallo weer!", value.Value);
            }
        }

        [TestMethod]
        public void QueryNoTracking()
        {
            using (var context = new MyContext(ConnectionString))
            {
                var query = context.Values.Where(r => new[] { 701, 702, 703, 704 }.Contains(r.Id)).AsNoTracking().ToList();


                Assert.IsNull(context.ChangeTracker.Entry(query[1]));
            }
        }

        [TestMethod]
        public void QueryContextNoTracking()
        {
            using (var context = new MyContext(ConnectionString))
            {
                context.TrackChanges = false;

                //query, and check if tracked
                var query = context.Values.Where(r => new[] { 701, 702, 703, 704 }.Contains(r.Id)).ToList();
                Assert.IsNull(context.ChangeTracker.Entry(query[1]));
            }
        }

        [TestMethod]
        public async Task FindContextNoTracking()
        {
            int count = 0;
            using (var context = new MyContext(ConnectionString))
            {
                context.TrackChanges = false;

                context.Database.Log = cql =>
                {
                    count++;
                    Debug.WriteLine("EXECUTE QUERY: " + cql);
                };

                //find and check if tracked
                var entity = await context.Values.FindAsync(1);
                Assert.IsNotNull(entity);
                Assert.IsNull(context.ChangeTracker.Entry(entity));

                //find again and check if tracked
                var entity2 = await context.Values.FindAsync(1);
                Assert.IsNotNull(entity);
                Assert.IsNull(context.ChangeTracker.Entry(entity2));

                //expecting two different objects
                Assert.AreNotSame(entity, entity2);

                //expecting two queries
                Assert.AreEqual(2, count);
            }
        }

        [TestMethod]
        public void CompileSimpleSelect()
        {
            Func<MyContext, MyValue> compiledQuery =
                CompiledQuery.Compile<MyContext, MyValue>(
                    (context) => context.Values.First(val => val.Id == 1));

            using (var context = new MyContext(ConnectionString))
            {
                var first = compiledQuery(context);
                Assert.IsNotNull(first);
                Assert.AreEqual(1, first.Id);
                Assert.AreEqual("Hallo 1", first.Value);
            }
        }

        [TestMethod]
        public void CompileSimpleSelectWithParam()
        {
            Func<MyContext, string, string> compiledQuery =
                CompiledQuery.Compile<MyContext, string, string>(
                    (context, append) => context.Values.Where(val => val.Id == 1).Select(val => val.Value + append).First());

            using (var context = new MyContext(ConnectionString))
            {
                var first = compiledQuery(context, " ook");
                Assert.IsNotNull(first);
                Assert.AreEqual("Hallo 1 ook", first);
            }
        }

       

        [TestMethod]
        public void CompileSingleArgument()
        {
            Func<MyContext, int, MyValue> compiledQuery =
                CompiledQuery.Compile<MyContext, int, MyValue>(
                    (context, id) => context.Values.Single(val => val.Id == id));

            using (var context = new MyContext(ConnectionString))
            {
                var singleValue = compiledQuery(context, 2);
                Assert.IsNotNull(singleValue);
                Assert.AreEqual(2, singleValue.Id);
                Assert.AreEqual("Hallo 2", singleValue.Value);
            }
        }

        [TestMethod]
        public void CompileAny()
        {
            Func<MyContext, int, bool> compiledQuery =
                CompiledQuery.Compile<MyContext, int, bool>(
                    (context, id) => context.Values.Any(val => val.Id == id));

            using (var context = new MyContext(ConnectionString))
            {
                var any = compiledQuery(context, 2);
                Assert.AreEqual(true, any);
            }
        }

        [TestMethod]
        public void CompileCount()
        {
            Func<MyContext, int, int> compiledQuery =
                CompiledQuery.Compile<MyContext, int, int>(
                    (context, id) => context.Values.Count(val => val.Id == id));

            using (var context = new MyContext(ConnectionString))
            {
                var count = compiledQuery(context, 2);
                Assert.AreEqual(1, count);
            }
        }

        [TestMethod]
        public void CompileLongCount()
        {
            Func<MyContext, int, long> compiledQuery =
                CompiledQuery.Compile<MyContext, int, long>(
                    (context, id) => context.Values.LongCount(val => val.Id == id));

            using (var context = new MyContext(ConnectionString))
            {
                var count = compiledQuery(context, 2);
                Assert.AreEqual(1, count);
            }
        }

        [TestMethod]
        [ExpectedException(typeof(CqlLinqException))]
        public void CompileCountPlusOne()
        {
            Func<MyContext, int, int> compiledQuery =
               CompiledQuery.Compile<MyContext, int, int>(
                   (context, id) => context.Values.Count(val => val.Id == id) + 1);
           
        }

        [TestMethod]
        public void CompileWhereContainsToDictionary()
        {

            Func<MyContext, IEnumerable<int>, IDictionary<int, MyValue>> compiledQuery =
                CompiledQuery.Compile<MyContext, IEnumerable<int>, IDictionary<int, MyValue>>
                (
                    (context, ids) => context.Values.Where(r => ids.Contains(r.Id)).ToDictionary(v => v.Id)
                );

            using (var context = new MyContext(ConnectionString))
            {
                var values = compiledQuery(context, new[] { 1, 2, 3, 4 });

                Assert.AreEqual(4, values.Count, "Unexpected number of results");
                for (int i = 1; i <= 4; i++)
                {
                    Assert.IsTrue(values.Keys.Any(key => key == i), "Missing value " + i);
                }
            }
        }
    }
}