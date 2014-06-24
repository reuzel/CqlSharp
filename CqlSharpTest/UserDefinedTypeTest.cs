using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqlSharp;
using CqlSharp.Serialization.Marshal;
using CqlSharp.Serialization;
using System.Collections.Generic;
using CqlSharp.Protocol;

namespace CqlSharp.Test
{
    [TestClass]
    public class UserDefinedTypeTest
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

            const string createTableCql = @"create table TestUDT.Members (id int primary key, user user, comment text);";

            const string createTable2Cql = @"create table TestUDT.Groups (id int primary key, members set<user>);";

            using (var connection = new CqlConnection(ConnectionString))
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
                catch (AlreadyExistsException)
                {
                }
            }
        }

        [ClassCleanup]
        public static void Cleanup()
        {
            const string dropCql = @"drop keyspace TestUDT;";

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

        [TestInitialize]
        public void PrepareTest()
        {
            const string truncateTableCql = @"truncate TestUDT.Members;";
            const string truncateTable2Cql = @"truncate TestUDT.Groups;";

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();
                var truncTable = new CqlCommand(connection, truncateTableCql);
                truncTable.ExecuteNonQuery();

                var truncTable2 = new CqlCommand(connection, truncateTable2Cql);
                truncTable2.ExecuteNonQuery();
            }
        }

        [TestMethod]
        public void InsertUser()
        {
            var address = new Address { Street = "MyWay", Number = 1 };
            var user = new User { Name = "Joost", Password = new byte[] { 1, 2, 3 }, Address = address };
            var member = new Member { Id = 1, User = user, Comment = "phew" };

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                var command = new CqlCommand(connection, "insert into testudt.members (id, user, comment) values (?,?,?);");
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
        public void InsertGroup()
        {
            var address = new Address { Street = "MyWay", Number = 1 };
            var user = new User { Name = "Joost", Password = new byte[] { 1, 2, 3 }, Address = address };
            var group = new Group { Id = 1, Members = new HashSet<User> { user } };

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                var command = new CqlCommand(connection, "insert into testudt.groups (id, members) values (?,?);");
                command.Prepare();
                command.Parameters.Set(group);
                command.ExecuteNonQuery();

                var select = new CqlCommand(connection, "select * from testudt.groups;");
                using (var reader = select.ExecuteReader<Group>())
                {
                    Assert.AreEqual(1, reader.Count);
                    if (reader.Read())
                    {
                        var actual = reader.Current;

                        Assert.IsNotNull(actual);
                        Assert.AreEqual(group.Id, actual.Id);
                        Assert.IsNotNull(group.Members);
                        Assert.IsInstanceOfType(group.Members, typeof(HashSet<User>));
                        Assert.AreEqual(1, group.Members.Count);
                        Assert.AreEqual("Joost", group.Members.First().Name);
                    }
                }
            }
        }

        [CqlUserType]
        [CqlEntity("address", Keyspace="testudt")]
        public class Address
        {
            [CqlColumn(Order=0)]
            public string Street { get; set; }

            [CqlColumn(Order = 1)]
            public int Number { get; set; }
        }

        [CqlUserType]
        [CqlEntity("user", Keyspace = "testudt")]
        class User
        {
            [CqlColumn(Order = 0)]
            public string Name { get; set; }

            [CqlColumn(Order = 1)]
            public byte[] Password { get; set; }

            [CqlColumn(Order = 2)]
            public Address Address { get; set; }
        }

        [CqlTable("members", Keyspace="testudt")]
        class Member
        {
            [CqlKey]
            [CqlColumn(Order=0)]
            public int Id { get; set; }

            [CqlColumn(Order=1)]
            public User User { get; set; }

            [CqlColumn(Order=2)]
            public string Comment { get; set; }
        }

        [CqlTable("groups", Keyspace = "testudt")]
        class Group
        {
            [CqlKey]
            [CqlColumn(Order = 0)]
            public int Id { get; set; }

            [CqlColumn(Order = 1)]
            public HashSet<User> Members { get; set; }
        }
    }
}
