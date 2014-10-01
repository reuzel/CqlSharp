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
using System.Collections.Generic;
using System.Linq;
using CqlSharp.Protocol;
using CqlSharp.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Test
{
    [TestClass]
    public class SerializationTest210
    {
        public static bool Cassandra210OrUp;
        
        private const string ConnectionString =
            "server=localhost;throttle=256;MaxConnectionIdleTime=3600;ConnectionStrategy=Exclusive;loggerfactory=debug;loglevel=query;username=cassandra;password=cassandra";

        [ClassInitialize]
        public static void Init(TestContext context)
        {
            const string createKsCql =
                @"CREATE KEYSPACE TestUDT WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = 'false';";

            const string createAddressType =
                @"create type TestUDT.TAddress (street text, number int);";

            const string createUserType =
                @"CREATE type TestUDT.TUser (name text, password blob, address frozen<TAddress>, phones list<text>);";

            const string createTableCql =
                @"create table TestUDT.Members (id int primary key, user frozen<TUser>, comment frozen<tuple<text,text>>);";

            const string createTable2Cql =
                @"create table TestUDT.Groups (id int primary key, members set<frozen<tuple<int, frozen<TUser>>>>);";

            using(var connection = new CqlConnection(ConnectionString))
            {
                connection.SetConnectionTimeout(0);
                connection.Open();

                Cassandra210OrUp = string.CompareOrdinal(connection.ServerVersion, "2.1.0") >= 0;
                if(!Cassandra210OrUp)
                    return;

                try
                {
                    var createKs = new CqlCommand(connection, createKsCql);
                    createKs.ExecuteNonQuery();

                    var createAddress = new CqlCommand(connection, createAddressType);
                    createAddress.ExecuteNonQuery();

                    var createUser = new CqlCommand(connection, createUserType);
                    createUser.ExecuteNonQuery();

                    var createTable = new CqlCommand(connection, createTableCql);
                    createTable.ExecuteNonQuery();

                    var createTable2 = new CqlCommand(connection, createTable2Cql);
                    createTable2.ExecuteNonQuery();
                }
                catch(AlreadyExistsException)
                {
                }
            }
        }

        [ClassCleanup]
        public static void Cleanup()
        {
            if (!Cassandra210OrUp)
                return;

            const string dropCql = @"drop keyspace TestUDT;";

            using(var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                try
                {
                    var drop = new CqlCommand(connection, dropCql);
                    drop.ExecuteNonQuery();
                }
                catch(InvalidException)
                {
                    //ignore
                }
            }

            CqlConnection.ShutdownAll();
        }

        [TestInitialize]
        public void PrepareTest()
        {
            if (!Cassandra210OrUp)
                return;

            const string truncateTableCql = @"truncate TestUDT.Members;";
            const string truncateTable2Cql = @"truncate TestUDT.Groups;";

            using(var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();
                var truncTable = new CqlCommand(connection, truncateTableCql);
                truncTable.ExecuteNonQuery();

                var truncTable2 = new CqlCommand(connection, truncateTable2Cql);
                truncTable2.ExecuteNonQuery();
            }
        }

        [TestMethod]
        public void InsertAndSelectTupleAndUDT()
        {
            if (!Cassandra210OrUp)
                return;

            var address = new Address {Street = "MyWay", Number = 1};
            var user = new User { Name = "Joost", Password = new byte[] { 1, 2, 3 }, Address = address, Phones = new List<string> { "call me once", "call me twice", "no answer" } };
            var member = new Member { Id = 1, User = user, Comment = Tuple.Create("my title", "phew") };

            using(var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                var command = new CqlCommand(connection,
                                             "insert into testudt.members (id, user, comment) values (?,?,?);");
                command.Prepare();
                command.Parameters.Set(member);
                command.ExecuteNonQuery();

                var select = new CqlCommand(connection, "select * from testudt.members;");
                using(var reader = select.ExecuteReader<Member>())
                {
                    Assert.AreEqual(1, reader.Count);
                    if(reader.Read())
                    {
                        var actual = reader.Current;

                        Assert.IsNotNull(actual);
                        Assert.AreEqual(member.Id, actual.Id);
                        Assert.AreEqual(member.Comment, actual.Comment);
                        Assert.IsNotNull(member.User);
                        Assert.AreEqual(member.User.Name, actual.User.Name);
                        Assert.IsNotNull(member.User.Address);
                        Assert.AreEqual(member.User.Address.Street, actual.User.Address.Street);
                        Assert.IsNotNull(member.User.Phones);
                        Assert.AreEqual(3, member.User.Phones.Count);
                        Assert.AreEqual("call me twice", member.User.Phones[1]);

                    }
                    else
                    {
                        Assert.Fail("Read failed.");
                    }

                }
            }
        }

        [TestMethod]
        public void InsertAndSelectNestedUDTAndTuples()
        {
            if (!Cassandra210OrUp)
                return;

            var address = new Address {Street = "MyWay", Number = 1};
            var user = new User { Name = "Joost", Password = new byte[] { 1, 2, 3 }, Address = address, Phones = new List<string> { "call me once", "call me twice", "no answer" } };
            var group = new Group { Id = 1, Members = new HashSet<Tuple<int, User>> { Tuple.Create(1, user) } };

            using(var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                var command = new CqlCommand(connection, "insert into testudt.groups (id, members) values (?,?);");
                command.Prepare();
                command.Parameters.Set(group);
                command.ExecuteNonQuery();

                var select = new CqlCommand(connection, "select * from testudt.groups;");
                using(var reader = select.ExecuteReader<Group>())
                {
                    Assert.AreEqual(1, reader.Count);
                    if(reader.Read())
                    {
                        var actual = reader.Current;

                        Assert.IsNotNull(actual);
                        Assert.AreEqual(group.Id, actual.Id);
                        Assert.IsNotNull(group.Members);
                        Assert.IsInstanceOfType(group.Members, typeof(HashSet<Tuple<int, User>>));
                        Assert.AreEqual(1, group.Members.Count);
                        Assert.AreEqual("Joost", group.Members.First().Item2.Name);
                        Assert.AreEqual("call me twice", group.Members.First().Item2.Phones[1]);
                    }
                    else
                    {
                        Assert.Fail("Read failed.");
                    }

                }
            }
        }

        [TestMethod]
        public void SelectAnonymousUDT()
        {
            if (!Cassandra210OrUp)
                return;

            var address = new Address {Street = "MyWay", Number = 1};
            var user = new User { Name = "Joost", Password = new byte[] { 1, 2, 3 }, Address = address, Phones = new List<string> { "call me once", "call me twice", "no answer" } };
            var member = new Member {Id = 1, User = user, Comment = Tuple.Create("my title", "phew")};

            using(var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                var command = new CqlCommand(connection,
                                             "insert into testudt.members (id, user, comment) values (?,?,?);");
                command.Prepare();
                command.Parameters.Set(member);
                command.ExecuteNonQuery();

                var select = new CqlCommand(connection, "select id, user, comment from testudt.members;");
                using(var reader = select.ExecuteReader())
                {
                    Assert.AreEqual(1, reader.Count);
                    if(reader.Read())
                    {
                        dynamic actualUser = reader.GetUserDefinedType(1);

                        Assert.IsNotNull(actualUser);
                        Assert.AreEqual(member.User.Name, actualUser.Name);
                        Assert.IsNotNull(actualUser.Address);
                        Assert.AreEqual(member.User.Address.Street, actualUser.Address.Street);
                        Assert.AreEqual(member.User.Phones[2], actualUser.Phones[2]);
                    }
                    else
                    {
                        Assert.Fail("Read failed.");
                    }

                }
            }
        }

        [TestMethod]
        public void SelectUDTAndTuplesViaNonGenericReader()
        {
            if (!Cassandra210OrUp)
                return;

            var address = new Address {Street = "MyWay", Number = 1};
            var user = new User { Name = "Joost", Password = new byte[] { 1, 2, 3 }, Address = address, Phones = new List<string> { "call me once", "call me twice", "no answer" } };
            var member = new Member {Id = 1, User = user, Comment = Tuple.Create("my title", "phew")};

            using(var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                var command = new CqlCommand(connection,
                                             "insert into testudt.members (id, user, comment) values (?,?,?);");
                command.Prepare();
                command.Parameters.Set(member);
                command.ExecuteNonQuery();

                var select = new CqlCommand(connection, "select id, user, comment from testudt.members;");
                using(var reader = select.ExecuteReader())
                {
                    Assert.AreEqual(1, reader.Count);
                    if(reader.Read())
                    {
                        var actualUser = reader.GetUserDefinedType<User>(1);

                        Assert.IsNotNull(actualUser);
                        Assert.AreEqual(member.User.Name, actualUser.Name);
                        Assert.IsNotNull(actualUser.Address);
                        Assert.AreEqual(member.User.Address.Street, actualUser.Address.Street);
                        Assert.IsNotNull(actualUser.Phones);
                        Assert.AreEqual(member.User.Phones[2], actualUser.Phones[2]);

                        var comment = reader.GetTuple<string, string>(2);
                        Assert.IsNotNull(comment);
                        Assert.AreEqual(member.Comment, comment);
                    }
                    else
                    {
                        Assert.Fail("Read failed.");
                    }

                }
            }
        }

        [TestMethod]
        public void SerializeTupleAndUDTOutNullTest()
        {
            if (!Cassandra210OrUp)
                return;

            using(var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                var command = new CqlCommand(connection, "insert into testudt.members (id) values (1);");
                command.ExecuteNonQuery();

                var select = new CqlCommand(connection, "select * from testudt.members;");
                using(var reader = select.ExecuteReader<Member>())
                {
                    Assert.AreEqual(1, reader.Count);
                    if(reader.Read())
                    {
                        Assert.IsNull(reader.GetUserDefinedType(1));
                        Assert.IsNull(reader.GetTuple<string, string>(2));

                        var actual = reader.Current;
                        Assert.IsNotNull(actual);
                        Assert.AreEqual(1, actual.Id);
                        Assert.IsNull(actual.User);
                        Assert.IsNull(actual.Comment);
                    }
                    else
                    {
                        Assert.Fail("Read failed.");
                    }

                }
            }
        }

        [TestMethod]
        public void InsertWithTimestamp()
        {
            if (!Cassandra210OrUp)
                return;

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                DateTime yesterday = DateTime.UtcNow - TimeSpan.FromDays(1);

                var command = new CqlCommand(connection, "insert into testudt.members (id, comment) values (2,('hi','there'));");
                command.Timestamp = yesterday;
                command.ExecuteNonQuery();

                var select = new CqlCommand(connection, "select WRITETIME(comment) from testudt.members where id=2;");
                using (var reader = select.ExecuteReader())
                {
                    Assert.AreEqual(1, reader.Count);
                    if (reader.Read())
                    {
                        DateTime writeTime = ((long)(reader.GetDouble(0) / 1000)).ToDateTime();
                        Assert.AreEqual(0 , (int)((writeTime-yesterday).TotalMilliseconds));
                    }
                    else
                    {
                        Assert.Fail("Read failed.");
                    }
                }
            }
        }

        [TestMethod]
        public void InsertWithTimestamp2()
        {
            if (!Cassandra210OrUp)
                return;

            using (var connection = new CqlConnection(ConnectionString))
            {
               connection.Open();

               var command = new CqlCommand(connection, "insert into testudt.members (id, comment) values (3,('hi','there3'));");
               command.ExecuteNonQuery();

                var select = new CqlCommand(connection, "select WRITETIME(comment) from testudt.members where id=3;");
                using (var reader = select.ExecuteReader())
                {
                    Assert.AreEqual(1, reader.Count);
                    if (reader.Read())
                    {
                        DateTime writeTime = ((long)(reader.GetDouble(0)/1000)).ToDateTime();
                        Assert.IsTrue(writeTime - DateTime.UtcNow < TimeSpan.FromSeconds(1));
                    }
                    else
                    {
                        Assert.Fail("Read failed.");
                    }
                }
            }
        }

        [CqlUserType("testudt", "taddress")]
        public class Address
        {
            [CqlColumn(Order = 0)]
            public string Street { get; set; }

            [CqlColumn(Order = 1)]
            public int Number { get; set; }
        }

        [CqlUserType("testudt", "tuser")]
        private class User
        {
            [CqlColumn(Order = 0)]
            public string Name { get; set; }

            [CqlColumn(Order = 1)]
            public byte[] Password { get; set; }

            [CqlColumn(Order = 2)]
            public Address Address { get; set; }

            [CqlColumn(Order=3)]
            public List<String> Phones { get; set; }
        }

        [CqlTable("members", Keyspace = "testudt")]
        private class Member
        {
            [CqlKey]
            [CqlColumn(Order = 0)]
            public int Id { get; set; }

            [CqlColumn(Order = 1)]
            public User User { get; set; }

            [CqlColumn(Order = 2)]
            public Tuple<string, string> Comment { get; set; }
        }

        [CqlTable("groups", Keyspace = "testudt")]
        private class Group
        {
            [CqlKey]
            [CqlColumn(Order = 0)]
            public int Id { get; set; }

            [CqlColumn(Order = 1)]
            public HashSet<Tuple<int, User>> Members { get; set; }
        }
    }
}