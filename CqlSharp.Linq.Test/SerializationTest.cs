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
using CqlSharp.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Numerics;

namespace CqlSharp.Linq.Test
{
    [TestClass]
    public class SerializationTest
    {
        private const string ConnectionString =
            "server=localhost;loggerfactory=debug;loglevel=Verbose;username=cassandra;password=cassandra";

        private const string CreateKsCql =
            @"CREATE KEYSPACE LinqTest WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = 'false';";

        private const string CreateTableCql = @"
            create table LinqTest.Types (
                aInt int primary key,
                aLong bigint,
                aVarint varint,
                aTextString text,
                aVarcharString varchar,
                aASCIIString ascii,
                aBlob blob,
                aBool boolean,
                aDecimal decimal,
                aDouble double,  
                aFloat float,  
                aTimestamp timestamp,
                aTimeUUID timeuuid,
                aUUID uuid,
                aInet inet,
                aList list<text>,
                aSet set<int>,
                aMap map<bigint, text>);
";

        private const string TruncateTableCql = @"truncate LinqTest.Types;";

        [ClassInitialize]
        public static void ClassInit(TestContext context)
        {
            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();
                try
                {
                    var createKs = new CqlCommand(connection, CreateKsCql);
                    createKs.ExecuteNonQuery();
                }
                catch (AlreadyExistsException)
                {
                    //ignore
                }

                try
                {
                    var createTable = new CqlCommand(connection, CreateTableCql);
                    createTable.ExecuteNonQuery();
                }
                catch (AlreadyExistsException)
                {
                    //ignore
                }
            }
        }

        [TestInitialize]
        public void Init()
        {
            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                var truncTable = new CqlCommand(connection, TruncateTableCql);
                truncTable.ExecuteNonQuery();
            }
        }

        [ClassCleanup]
        public static void Cleanup()
        {
            const string dropCql = @"drop keyspace LinqTest;";

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
        public void SerializeObjectInOutTest()
        {
            var values = new Types
                                 {
                                     aASCIIString = "hello world!",
                                     aBlob = new byte[] { 1, 2, 3, 4 },
                                     aBool = true,
                                     aDecimal = decimal.MaxValue / 2,
                                     aDouble = 1.234,
                                     aFloat = 5.789f,
                                     aInet = new IPAddress(new byte[] { 127, 0, 0, 1 }),
                                     aInt = 10,
                                     aLong = 56789012456,
                                     aTextString = "some other text with \u005C unicode",
                                     aVarcharString = "some other varchar with \u005C unicode",
                                     aTimeUUID = DateTime.Now.GenerateTimeBasedGuid(),
                                     aUUID = Guid.NewGuid(),
                                     aTimestamp = DateTime.Now,
                                     aVarint = new BigInteger(12345678901234),
                                     aList = new List<string> { "string 1", "string 2" },
                                     aSet = new HashSet<int> { 1, 3, 3 },
                                     aMap =
                                         new Dictionary<long, string> { { 1, "value 1" }, { 2, "value 2" }, { 3, "value 3" } },
                                 };

            using (var context = new SerializationContext())
            {
                context.Types.Add(values);
                context.SaveChanges();
            }

            using (var context = new SerializationContext())
            {
                var result = context.Types.FirstOrDefault();

                Assert.IsNotNull(result);

                Assert.AreEqual(values.aASCIIString, result.aASCIIString);
                Assert.AreEqual(values.aVarcharString, result.aVarcharString);
                Assert.AreEqual(values.aVarint, result.aVarint);
                Assert.AreEqual(values.aTextString, result.aTextString);
                Assert.AreEqual(values.aBool, result.aBool);
                Assert.AreEqual(values.aDecimal, result.aDecimal);
                Assert.AreEqual(values.aDouble, result.aDouble);
                Assert.AreEqual(values.aFloat, result.aFloat);
                Assert.AreEqual(values.aInet, result.aInet);
                Assert.AreEqual(values.aInt, result.aInt);
                Assert.AreEqual(values.aLong, result.aLong);
                Assert.AreEqual(values.aTimeUUID, result.aTimeUUID);
                Assert.AreEqual(values.aUUID, result.aUUID);
                Assert.IsTrue(result.aBlob.SequenceEqual(values.aBlob));
                Assert.IsTrue(result.aList.SequenceEqual(values.aList));
                Assert.IsTrue(result.aSet.SequenceEqual(values.aSet));
            }
        }


        [TestMethod]
        public void SerializeObjectOutDefaultsTest()
        {
            const string insertCql = @"insert into LinqTest.Types(aInt) values (1);";

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                var insertCmd = new CqlCommand(connection, insertCql);
                insertCmd.ExecuteNonQuery();
            }

            using (var context = new SerializationContext())
            {
                var result = context.Types.FirstOrDefault();

                Assert.IsNotNull(result);

                Assert.AreEqual(result.aASCIIString, default(string));
                Assert.AreEqual(result.aVarcharString, default(string));
                Assert.AreEqual(result.aVarint, default(BigInteger));
                Assert.AreEqual(result.aTextString, default(string));
                Assert.AreEqual(result.aBool, default(bool));
                Assert.AreEqual(result.aDecimal, default(decimal));
                Assert.AreEqual(result.aDouble, default(double));
                Assert.AreEqual(result.aFloat, default(float));
                Assert.AreEqual(result.aInet, default(IPAddress));
                Assert.AreEqual(result.aLong, default(long));
                Assert.AreEqual(result.aTimeUUID, default(Guid));
                Assert.AreEqual(result.aUUID, default(Guid));
                Assert.AreEqual(result.aBlob, default(byte[]));
                Assert.AreEqual(result.aList, default(List<string>));
                Assert.AreEqual(result.aSet, default(HashSet<int>));
                Assert.AreEqual(result.aMap, default(Dictionary<long, string>));

            }
        }

        [TestMethod]
        public void SerializeObjectInOutDefaultsTest()
        {
            var values = new Types { aInt = 3, aTimeUUID = TimeGuid.Default };

            using (var context = new SerializationContext())
            {
                context.Types.Add(values);
                context.SaveChanges();
            }

            using (var context = new SerializationContext())
            {
                var result = context.Types.FirstOrDefault();

                Assert.IsNotNull(result);

                Assert.AreEqual(result.aASCIIString, default(string));
                Assert.AreEqual(result.aVarcharString, default(string));
                Assert.AreEqual(result.aVarint, default(BigInteger));
                Assert.AreEqual(result.aTextString, default(string));
                Assert.AreEqual(result.aBool, default(bool));
                Assert.AreEqual(result.aDecimal, default(decimal));
                Assert.AreEqual(result.aDouble, default(double));
                Assert.AreEqual(result.aFloat, default(float));
                Assert.AreEqual(result.aInet, default(IPAddress));
                Assert.AreEqual(result.aLong, default(long));
                Assert.AreEqual(result.aTimeUUID, TimeGuid.Default);
                Assert.AreEqual(result.aUUID, default(Guid));
                Assert.AreEqual(result.aBlob, default(byte[]));
                Assert.AreEqual(result.aList, default(List<string>));
                Assert.AreEqual(result.aSet, default(HashSet<int>));
                Assert.AreEqual(result.aMap, default(Dictionary<long, string>));
            }
        }

        [TestMethod]
        public void SerializeNullableObjectInOutTest()
        {
            var values = new NullableTypes
            {
                aASCIIString = "hello world!",
                aBlob = new byte[] { 1, 2, 3, 4 },
                aBool = true,
                aDecimal = decimal.MaxValue / 2,
                aDouble = 1.234,
                aFloat = 5.789f,
                aInet = new IPAddress(new byte[] { 127, 0, 0, 1 }),
                aInt = 10,
                aLong = 56789012456,
                aTextString = "some other text with \u005C unicode",
                aVarcharString = "some other varchar with \u005C unicode",
                aTimeUUID = DateTime.Now.GenerateTimeBasedGuid(),
                aUUID = Guid.NewGuid(),
                aTimestamp = DateTime.Now,
                aVarint = new BigInteger(12345678901234),
                aList = new List<string> { "string 1", "string 2" },
                aSet = new HashSet<int> { 1, 3, 3 },
                aMap =
                    new Dictionary<long, string> { { 1, "value 1" }, { 2, "value 2" }, { 3, "value 3" } },
            };

            using (var context = new SerializationContext())
            {
                context.NullableTypes.Add(values);
                context.SaveChanges();
            }

            using (var context = new SerializationContext())
            {
                var result = context.NullableTypes.FirstOrDefault();

                Assert.IsNotNull(result);

                Assert.AreEqual(values.aASCIIString, result.aASCIIString);
                Assert.AreEqual(values.aVarcharString, result.aVarcharString);
                Assert.AreEqual(values.aVarint, result.aVarint);
                Assert.AreEqual(values.aTextString, result.aTextString);
                Assert.AreEqual(values.aBool, result.aBool);
                Assert.AreEqual(values.aDecimal, result.aDecimal);
                Assert.AreEqual(values.aDouble, result.aDouble);
                Assert.AreEqual(values.aFloat, result.aFloat);
                Assert.AreEqual(values.aInet, result.aInet);
                Assert.AreEqual(values.aInt, result.aInt);
                Assert.AreEqual(values.aLong, result.aLong);
                Assert.AreEqual(values.aTimeUUID, result.aTimeUUID);
                Assert.AreEqual(values.aUUID, result.aUUID);
                Assert.IsTrue(result.aBlob.SequenceEqual(values.aBlob));
                Assert.IsTrue(result.aList.SequenceEqual(values.aList));
                Assert.IsTrue(result.aSet.SequenceEqual(values.aSet));
            }
        }


        [TestMethod]
        public void SerializeNullableObjectOutDefaultsTest()
        {
            const string insertCql = @"insert into LinqTest.Types(aInt) values (1);";

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                var insertCmd = new CqlCommand(connection, insertCql);
                insertCmd.ExecuteNonQuery();
            }

            using (var context = new SerializationContext())
            {
                var result = context.NullableTypes.FirstOrDefault();

                Assert.IsNotNull(result);

                Assert.IsNull(result.aASCIIString);
                Assert.IsNull(result.aVarcharString);
                Assert.IsNull(result.aVarint);
                Assert.IsNull(result.aTextString);
                Assert.IsNull(result.aBool);
                Assert.IsNull(result.aDecimal);
                Assert.IsNull(result.aDouble);
                Assert.IsNull(result.aFloat);
                Assert.IsNull(result.aInet);
                Assert.IsNull(result.aLong);
                Assert.IsNull(result.aTimeUUID);
                Assert.IsNull(result.aUUID);
                Assert.IsNull(result.aBlob);
                Assert.IsNull(result.aList);
                Assert.IsNull(result.aSet);
                Assert.IsNull(result.aMap);

            }
        }

        [TestMethod]
        public void SerializeNullableObjectInOutDefaultsTest()
        {
            var values = new NullableTypes { aInt = 10 };

            using (var context = new SerializationContext())
            {
                context.NullableTypes.Add(values);
                context.SaveChanges();
            }

            using (var context = new SerializationContext())
            {
                var result = context.NullableTypes.FirstOrDefault();

                Assert.IsNotNull(result);

                Assert.IsNull(result.aASCIIString);
                Assert.IsNull(result.aVarcharString);
                Assert.IsNull(result.aVarint);
                Assert.IsNull(result.aTextString);
                Assert.IsNull(result.aBool);
                Assert.IsNull(result.aDecimal);
                Assert.IsNull(result.aDouble);
                Assert.IsNull(result.aFloat);
                Assert.IsNull(result.aInet);
                Assert.IsNull(result.aLong);
                Assert.IsNull(result.aTimeUUID);
                Assert.IsNull(result.aUUID);
                Assert.IsNull(result.aBlob);
                Assert.IsNull(result.aList);
                Assert.IsNull(result.aSet);
                Assert.IsNull(result.aMap);
            }
        }

        // ReSharper disable InconsistentNaming

        private class SerializationContext : CqlContext
        {
            public SerializationContext()
                : base(ConnectionString)
            {
            }

            public CqlTable<Types> Types { get; set; }

            public CqlTable<NullableTypes> NullableTypes { get; set; }
        }

        #region Nested type: NullableTypes

        [CqlTable("types", Keyspace = "linqtest")]
        public class NullableTypes
        {
            public int aInt { get; set; }
            public long? aLong { get; set; }
            public BigInteger? aVarint { get; set; }
            public string aTextString { get; set; }
            public string aVarcharString { get; set; }
            public string aASCIIString { get; set; }
            public byte[] aBlob { get; set; }
            public bool? aBool { get; set; }
            public decimal? aDecimal { get; set; }
            public double? aDouble { get; set; }
            public float? aFloat { get; set; }
            public DateTime? aTimestamp { get; set; }
            public Guid? aTimeUUID { get; set; }
            public Guid? aUUID { get; set; }
            public IPAddress aInet { get; set; }
            public List<string> aList { get; set; }
            public HashSet<int> aSet { get; set; }
            public Dictionary<long, string> aMap { get; set; }
        }

        #endregion

        #region Nested type: Types

        [CqlTable("types", Keyspace = "linqtest")]
        public class Types
        {
            public int aInt { get; set; }
            public long aLong { get; set; }
            public BigInteger aVarint { get; set; }
            public string aTextString { get; set; }
            public string aVarcharString { get; set; }
            public string aASCIIString { get; set; }
            public byte[] aBlob { get; set; }
            public bool aBool { get; set; }
            public decimal aDecimal { get; set; }
            public double aDouble { get; set; }
            public float aFloat { get; set; }
            public DateTime aTimestamp { get; set; }
            public Guid aTimeUUID { get; set; }
            public Guid aUUID { get; set; }
            public IPAddress aInet { get; set; }
            public List<string> aList { get; set; }
            public HashSet<int> aSet { get; set; }
            public Dictionary<long, string> aMap { get; set; }

        }

        #endregion

        // ReSharper restore InconsistentNaming
    }
}