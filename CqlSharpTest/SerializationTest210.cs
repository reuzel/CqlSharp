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
        private const string ConnectionString =
            "server=localhost;throttle=256;MaxConnectionIdleTime=3600;ConnectionStrategy=Exclusive;loggerfactory=debug;loglevel=query;username=cassandra;password=cassandra";

        [ClassInitialize]
        public static void Init(TestContext context)
        {
            const string createKsCql =
                @"CREATE KEYSPACE TestUDT WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = 'false';";

            const string createAddressType =
                @"create type TestUDT.address (street text, number int);";

            const string createUserType =
                @"CREATE type TestUDT.user (name text, password blob, address address);";

            const string createTableCql =
                @"create table TestUDT.Members (id int primary key, user user, comment tuple<text,text>);";

            const string createTable2Cql =
                @"create table TestUDT.Groups (id int primary key, members set<tuple<int, user>>);";

            using(var connection = new CqlConnection(ConnectionString))
            {
                connection.SetConnectionTimeout(0);
                connection.Open();

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
            var address = new Address {Street = "MyWay", Number = 1};
            var user = new User {Name = "Joost", Password = new byte[] {1, 2, 3}, Address = address};
            var member = new Member {Id = 1, User = user, Comment = Tuple.Create("my title", "phew")};

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
                    }
                }
            }
        }

        [TestMethod]
        public void InsertAndSelectNestedUDTAndTuples()
        {
            var address = new Address {Street = "MyWay", Number = 1};
            var user = new User {Name = "Joost", Password = new byte[] {1, 2, 3}, Address = address};
            var group = new Group {Id = 1, Members = new HashSet<Tuple<int, User>> {Tuple.Create(1, user)}};

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
                    }
                }
            }
        }

        [TestMethod]
        public void SelectAnonymousUDT()
        {
            var address = new Address {Street = "MyWay", Number = 1};
            var user = new User {Name = "Joost", Password = new byte[] {1, 2, 3}, Address = address};
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
                    }
                }
            }
        }

        [TestMethod]
        public void SelectUDTAndTuplesViaNonGenericReader()
        {
            var address = new Address {Street = "MyWay", Number = 1};
            var user = new User {Name = "Joost", Password = new byte[] {1, 2, 3}, Address = address};
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

                        var comment = reader.GetTuple<string, string>(2);
                        Assert.IsNotNull(comment);
                        Assert.AreEqual(member.Comment, comment);
                    }
                }
            }
        }

        [TestMethod]
        public void SerializeTupleAndUDTOutNullTest()
        {
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
                }
            }
        }

        [CqlUserType("testudt", "address")]
        public class Address
        {
            [CqlColumn(Order = 0)]
            public string Street { get; set; }

            [CqlColumn(Order = 1)]
            public int Number { get; set; }
        }

        [CqlUserType("testudt", "user")]
        private class User
        {
            [CqlColumn(Order = 0)]
            public string Name { get; set; }

            [CqlColumn(Order = 1)]
            public byte[] Password { get; set; }

            [CqlColumn(Order = 2)]
            public Address Address { get; set; }
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