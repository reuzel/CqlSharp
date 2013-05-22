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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Numerics;
using CqlSharp;
using CqlSharp.Protocol;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharpTest
{
    [TestClass]
    public class SerializationTest
    {
        private const string ConnectionString = "server=localhost;throttle=100;ConnectionStrategy=Exclusive";

        private const string CreateKsCql =
            @"CREATE KEYSPACE Test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = 'false';";

        private const string CreateTableCql = @"
            create table Test.Types (
                aInt int primary key,
                aLong bigint,
                aVarint varint,
                aTextString text,
                aVarcharString varchar,
                aASCIIString ascii,
                aBlob blob,
                aBool boolean,
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

        private const string TruncateTableCql = @"truncate Test.Types;";

        [TestInitialize]
        public void Init()
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
                    var truncTable = new CqlCommand(connection, TruncateTableCql);
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
        public void InsertTest()
        {
            const string insertCql = @"insert into Test.Types(
                aInt,
                aLong ,
                aVarint ,
                aTextString ,
                aVarcharString ,
                aASCIIString ,
                aBlob ,
                aBool ,
                aDouble  , 
                aFloat  , 
                aTimestamp ,
                aTimeUUID ,
                aUUID ,
                aInet ,
                aList,
                aSet,
                aMap) 
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

            const string selectCql = "select * from Test.Types limit 1;";

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                var values = new Types
                                 {
                                     aASCIIString = "hello world!",
                                     aBlob = new byte[] {1, 2, 3, 4},
                                     aBool = true,
                                     aDouble = 1.234,
                                     aFloat = 5.789f,
                                     aInet = new IPAddress(new byte[] {127, 0, 0, 1}),
                                     aInt = 10,
                                     aLong = 56789012456,
                                     aTextString = "some other text with \u005C unicode",
                                     aVarcharString = "some other varchar with \u005C unicode",
                                     aTimeUUID = DateTime.Now.GenerateTimeBasedGuid(),
                                     aUUID = Guid.NewGuid(),
                                     aTimestamp = DateTime.Now,
                                     aVarint = new BigInteger(12345678901234),
                                     aList = new List<string> {"string 1", "string 2"},
                                     aSet = new HashSet<int> {1, 3, 3},
                                     aMap =
                                         new Dictionary<long, string> {{1, "value 1"}, {2, "value 2"}, {3, "value 3"}},
                                 };

                var insertCmd = new CqlCommand(connection, insertCql);
                insertCmd.Prepare();
                insertCmd.Parameters.Set(values);
                insertCmd.ExecuteNonQuery();

                var selectCmd = new CqlCommand(connection, selectCql);
                Types result = null;
                using (var reader = selectCmd.ExecuteReader<Types>())
                {
                    if (reader.Read())
                        result = reader.Current;
                }

                Assert.IsNotNull(result);

                Assert.AreEqual(result.aASCIIString, values.aASCIIString);
                Assert.AreEqual(result.aVarcharString, values.aVarcharString);
                Assert.AreEqual(result.aVarint, values.aVarint);
                Assert.AreEqual(result.aTextString, values.aTextString);
                Assert.AreEqual(result.aBool, values.aBool);
                Assert.AreEqual(result.aDouble, values.aDouble);
                Assert.AreEqual(result.aFloat, values.aFloat);
                Assert.AreEqual(result.aInet, values.aInet);
                Assert.AreEqual(result.aInt, values.aInt);
                Assert.AreEqual(result.aLong, values.aLong);
                Assert.AreEqual(result.aTimeUUID, values.aTimeUUID);
                Assert.AreEqual(result.aUUID, values.aUUID);
                Assert.IsTrue(result.aBlob.SequenceEqual(values.aBlob));
                Assert.IsTrue(result.aList.SequenceEqual(values.aList));
                Assert.IsTrue(result.aSet.SequenceEqual(values.aSet));
            }
        }

        [TestMethod]
        public void NullDeserializeTest()
        {
            const string insertCql = @"insert into Test.Types(aInt) values (1);";

            const string selectCql = "select * from Test.Types limit 1;";

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();
                
                var insertCmd = new CqlCommand(connection, insertCql);
                insertCmd.ExecuteNonQuery();

                var selectCmd = new CqlCommand(connection, selectCql);
                Types result = null;
                using (var reader = selectCmd.ExecuteReader<Types>())
                {
                    if (reader.Read())
                        result = reader.Current;
                }

                Assert.IsNotNull(result);

                Assert.AreEqual(result.aASCIIString,default(string));
                Assert.AreEqual(result.aVarcharString, default(string));
                Assert.AreEqual(result.aVarint, default(BigInteger));
                Assert.AreEqual(result.aTextString, default(string));
                Assert.AreEqual(result.aBool, default(bool));
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
        public void NullSerializeTest()
        {
            const string insertCql = @"insert into Test.Types(
                aInt,
                aLong ,
                aVarint ,
                aTextString ,
                aVarcharString ,
                aASCIIString ,
                aBlob ,
                aBool ,
                aDouble  , 
                aFloat  , 
                aTimestamp ,
                aTimeUUID ,
                aUUID ,
                aInet ,
                aList,
                aSet,
                aMap) 
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

            const string selectCql = "select * from Test.Types limit 1;";

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                var values = new Types
                {
                    aInt = 2,
                    aASCIIString = default(string),
                    aBlob = default(byte[]),
                    aBool = default(bool),
                    aDouble = default(double),
                    aFloat = default(float),
                    aInet = default(IPAddress),
                    aLong = default(long),
                    aTextString = default(string),
                    aVarcharString = default(string),
                    aTimeUUID = default(Guid),
                    aUUID = default(Guid),
                    aTimestamp = default(DateTime),
                    aVarint = default(BigInteger),
                    aList = default(List<string>),
                    aSet = default(HashSet<int>),
                    aMap = default(Dictionary<long, string>)
                };

                var insertCmd = new CqlCommand(connection, insertCql);
                insertCmd.Prepare();
                insertCmd.Parameters.Set(values);
                insertCmd.ExecuteNonQuery();

                var selectCmd = new CqlCommand(connection, selectCql);
                Types result = null;
                using (var reader = selectCmd.ExecuteReader<Types>())
                {
                    if (reader.Read())
                        result = reader.Current;
                }

                Assert.IsNotNull(result);

                Assert.AreEqual(result.aASCIIString, default(string));
                Assert.AreEqual(result.aVarcharString, default(string));
                Assert.AreEqual(result.aVarint, default(BigInteger));
                Assert.AreEqual(result.aTextString, default(string));
                Assert.AreEqual(result.aBool, default(bool));
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

        #region Nested type: Types

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
    }
}