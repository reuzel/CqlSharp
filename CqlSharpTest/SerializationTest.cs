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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Numerics;
using CqlSharp.Protocol;
using CqlSharp.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Test
{
    [TestClass]
    public class SerializationTest
    {
        private const string ConnectionString =
            "server=localhost;throttle=100;ConnectionStrategy=Exclusive;loggerfactory=debug;username=cassandra;password=cassandra";

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
        public void SerializeObjectInOutTest()
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

                Assert.AreEqual(values.aASCIIString, result.aASCIIString);
                Assert.AreEqual(values.aVarcharString, result.aVarcharString);
                Assert.AreEqual(values.aVarint, result.aVarint);
                Assert.AreEqual(values.aTextString, result.aTextString);
                Assert.AreEqual(values.aBool, result.aBool);
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
        public void SerializeInOutTest()
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


                var insertCmd = new CqlCommand(connection, insertCql);
                insertCmd.Prepare();

                insertCmd.Parameters["aasciistring"].Value = "hello world!";
                insertCmd.Parameters["ablob"].Value = new byte[] {1, 2, 3, 4};
                insertCmd.Parameters["abool"].Value = true;
                insertCmd.Parameters["adouble"].Value = 1.234;
                insertCmd.Parameters["afloat"].Value = 5.789f;
                insertCmd.Parameters["ainet"].Value = new IPAddress(new byte[] {127, 0, 0, 1});
                insertCmd.Parameters["aint"].Value = 10;
                insertCmd.Parameters["along"].Value = 56789012456;
                insertCmd.Parameters["atextstring"].Value = "some other text with \u005C unicode";
                insertCmd.Parameters["avarcharstring"].Value = "some other varchar with \u005C unicode";
                insertCmd.Parameters["atimeuuid"].Value = DateTime.Now.GenerateTimeBasedGuid();
                insertCmd.Parameters["auuid"].Value = Guid.NewGuid();
                insertCmd.Parameters["atimestamp"].Value = DateTime.Now;
                insertCmd.Parameters["avarint"].Value = new BigInteger(12345678901234);
                insertCmd.Parameters["alist"].Value = new List<string> {"string 1", "string 2"};
                insertCmd.Parameters["aset"].Value = new HashSet<int> {1, 3, 3};
                insertCmd.Parameters["amap"].Value =
                    new Dictionary<long, string> {{1, "value 1"}, {2, "value 2"}, {3, "value 3"}};

                insertCmd.ExecuteNonQuery();

                string aAsciiString;
                string aVarcharString;
                BigInteger aVarint;
                string aTextString;
                bool aBool;
                double aDouble;
                float aFloat;
                IPAddress aInet;
                long aLong;
                Guid aTimeUUID;
                Guid aUUID;
                byte[] aBlob;
                List<string> aList;
                HashSet<int> aSet;
                Dictionary<long, string> aMap;

                var selectCmd = new CqlCommand(connection, selectCql);
                using (var reader = selectCmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        aAsciiString = reader.GetString(reader.GetOrdinal("aasciistring"));
                        aVarcharString = reader.GetString(reader.GetOrdinal("avarcharstring"));
                        aVarint = reader.GetBigInteger(reader.GetOrdinal("avarint"));
                        aTextString = reader.GetString(reader.GetOrdinal("atextstring"));
                        aBool = reader.GetBoolean(reader.GetOrdinal("abool"));

                        aDouble = reader.GetDouble(reader.GetOrdinal("adouble"));
                        aFloat = reader.GetFloat(reader.GetOrdinal("afloat"));
                        aInet = reader.GetIPAddress(reader.GetOrdinal("ainet"));
                        aLong = reader.GetInt64(reader.GetOrdinal("along"));
                        aTimeUUID = reader.GetGuid(reader.GetOrdinal("atimeuuid"));
                        aUUID = reader.GetGuid(reader.GetOrdinal("auuid"));
                        aBlob = reader.GetBytes(reader.GetOrdinal("ablob"));
                        aList = reader.GetList<string>(reader.GetOrdinal("alist"));
                        aSet = reader.GetSet<int>(reader.GetOrdinal("aset"));
                        aMap = reader.GetDictionary<long, string>(reader.GetOrdinal("amap"));
                    }
                    else
                        throw new Exception("No row returned!");
                }

                Assert.AreEqual(insertCmd.Parameters["aasciistring"].Value, aAsciiString);
                Assert.AreEqual(insertCmd.Parameters["avarcharstring"].Value, aVarcharString);
                Assert.AreEqual(insertCmd.Parameters["avarint"].Value, aVarint);
                Assert.AreEqual(insertCmd.Parameters["atextstring"].Value, aTextString);
                Assert.AreEqual(insertCmd.Parameters["abool"].Value, aBool);
                Assert.AreEqual(insertCmd.Parameters["adouble"].Value, aDouble);
                Assert.AreEqual(insertCmd.Parameters["afloat"].Value, aFloat);
                Assert.AreEqual(insertCmd.Parameters["ainet"].Value, aInet);
                Assert.AreEqual(insertCmd.Parameters["along"].Value, aLong);
                Assert.AreEqual(insertCmd.Parameters["atimeuuid"].Value, aTimeUUID);
                Assert.AreEqual(insertCmd.Parameters["auuid"].Value, aUUID);
                Assert.IsTrue(((byte[]) insertCmd.Parameters["ablob"].Value).SequenceEqual(aBlob));
                Assert.IsTrue(((List<string>) insertCmd.Parameters["alist"].Value).SequenceEqual(aList));
                Assert.IsTrue(((HashSet<int>) insertCmd.Parameters["aset"].Value).SequenceEqual(aSet));
                foreach (var entry in ((Dictionary<long, string>) insertCmd.Parameters["amap"].Value))
                {
                    var val = aMap[entry.Key];
                    Assert.AreEqual(entry.Value, val);
                }
            }
        }

        [TestMethod]
        public void SerializeObjectOutDefaultsTest()
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

        [TestMethod]
        public void SerializeOutNullTest()
        {
            const string insertCql = @"insert into Test.Types(aInt) values (1);";

            const string selectCql = "select * from Test.Types limit 1;";

            using (var connection = new CqlConnection(ConnectionString))
            {
                string aAsciiString;
                string aVarcharString;
                BigInteger? aVarint;
                string aTextString;
                bool? aBool;
                double? aDouble;
                float? aFloat;
                IPAddress aInet;
                long? aLong;
                Guid? aTimeUUID;
                Guid? aUUID;
                byte[] aBlob;
                List<string> aList;
                HashSet<int> aSet;
                Dictionary<long, string> aMap;

                connection.Open();

                var insertCmd = new CqlCommand(connection, insertCql);
                insertCmd.ExecuteNonQuery();

                var selectCmd = new CqlCommand(connection, selectCql);

                using (var reader = selectCmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        aAsciiString = reader["aasciistring"] as string;
                        aVarcharString = reader["avarcharstring"] as string;
                        aVarint = reader["avarint"] as BigInteger?;
                        aTextString = reader["atextstring"] as string;
                        aBool = reader["abool"] as bool?;
                        aDouble = reader["adouble"] as double?;
                        aFloat = reader["afloat"] as float?;
                        aInet = reader["ainet"] as IPAddress;
                        aLong = reader["along"] as long?;
                        aTimeUUID = reader["atimeuuid"] as Guid?;
                        aUUID = reader["auuid"] as Guid?;
                        aBlob = reader["ablob"] as byte[];
                        aList = reader["alist"] as List<string>;
                        aSet = reader["aset"] as HashSet<int>;
                        aMap = reader["amap"] as Dictionary<long, string>;
                    }
                    else
                        throw new Exception("No row returned!");
                }

                Assert.AreEqual(aAsciiString, default(string));
                Assert.AreEqual(aVarcharString, default(string));
                Assert.AreEqual(aVarint, default(BigInteger?));
                Assert.AreEqual(aTextString, default(string));
                Assert.AreEqual(aBool, default(bool?));
                Assert.AreEqual(aDouble, default(double?));
                Assert.AreEqual(aFloat, default(float?));
                Assert.AreEqual(aInet, default(IPAddress));
                Assert.AreEqual(aLong, default(long?));
                Assert.AreEqual(aTimeUUID, default(Guid?));
                Assert.AreEqual(aUUID, default(Guid?));
                Assert.AreEqual(aBlob, default(byte[]));
                Assert.AreEqual(aList, default(List<string>));
                Assert.AreEqual(aSet, default(HashSet<int>));
                Assert.AreEqual(aMap, default(Dictionary<long, string>));
            }
        }

        [TestMethod]
        public void SerializeOutDefaultTest()
        {
            const string insertCql = @"insert into Test.Types(aInt) values (1);";
            const string selectCql = "select * from Test.Types limit 1;";

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                var insertCmd = new CqlCommand(connection, insertCql);
                insertCmd.ExecuteNonQuery();

                string aAsciiString;
                string aVarcharString;
                BigInteger aVarint;
                string aTextString;
                bool aBool;
                double aDouble;
                float aFloat;
                IPAddress aInet;
                long aLong;
                Guid aTimeUUID;
                Guid aUUID;
                byte[] aBlob;
                List<string> aList;
                HashSet<int> aSet;
                Dictionary<long, string> aMap;

                var selectCmd = new CqlCommand(connection, selectCql);

                using (var reader = selectCmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        aAsciiString = reader.GetString(reader.GetOrdinal("aasciistring"));
                        aVarcharString = reader.GetString(reader.GetOrdinal("avarcharstring"));
                        aVarint = reader.GetBigInteger(reader.GetOrdinal("avarint"));
                        aTextString = reader.GetString(reader.GetOrdinal("atextstring"));
                        aBool = reader.GetBoolean(reader.GetOrdinal("abool"));

                        aDouble = reader.GetDouble(reader.GetOrdinal("adouble"));
                        aFloat = reader.GetFloat(reader.GetOrdinal("afloat"));
                        aInet = reader.GetIPAddress(reader.GetOrdinal("ainet"));
                        aLong = reader.GetInt64(reader.GetOrdinal("along"));
                        aTimeUUID = reader.GetGuid(reader.GetOrdinal("atimeuuid"));
                        aUUID = reader.GetGuid(reader.GetOrdinal("auuid"));
                        aBlob = reader.GetBytes(reader.GetOrdinal("ablob"));
                        aList = reader.GetList<string>(reader.GetOrdinal("alist"));
                        aSet = reader.GetSet<int>(reader.GetOrdinal("aset"));
                        aMap = reader.GetDictionary<long, string>(reader.GetOrdinal("amap"));
                    }
                    else
                        throw new Exception("No row returned!");
                }

                Assert.AreEqual(aAsciiString, default(string));
                Assert.AreEqual(aVarcharString, default(string));
                Assert.AreEqual(aVarint, default(BigInteger));
                Assert.AreEqual(aTextString, default(string));
                Assert.AreEqual(aBool, default(bool));
                Assert.AreEqual(aDouble, default(double));
                Assert.AreEqual(aFloat, default(float));
                Assert.AreEqual(aInet, default(IPAddress));
                Assert.AreEqual(aLong, default(long));
                Assert.AreEqual(aTimeUUID, default(Guid));
                Assert.AreEqual(aUUID, default(Guid));
                Assert.AreEqual(aBlob, default(byte[]));
                Assert.AreEqual(aList, default(List<string>));
                Assert.AreEqual(aSet, default(HashSet<int>));
                Assert.AreEqual(aMap, default(Dictionary<long, string>));
            }
        }

        [TestMethod]
        public void SerializeObjectInOutDefaultsTest()
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

        [TestMethod]
        public void SerializeObjectInOutNullTest()
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

                var values = new NullableTypes
                                 {
                                     aInt = 3,
                                     aASCIIString = null,
                                     aBlob = null,
                                     aBool = null,
                                     aDouble = null,
                                     aFloat = null,
                                     aInet = null,
                                     aLong = null,
                                     aTextString = null,
                                     aVarcharString = null,
                                     aTimeUUID = null,
                                     aUUID = null,
                                     aTimestamp = null,
                                     aVarint = null,
                                     aList = null,
                                     aSet = null,
                                     aMap = null
                                 };

                var insertCmd = new CqlCommand(connection, insertCql);
                insertCmd.Prepare();
                insertCmd.Parameters.Set(values);
                insertCmd.ExecuteNonQuery();

                var selectCmd = new CqlCommand(connection, selectCql);
                NullableTypes result = null;
                using (var reader = selectCmd.ExecuteReader<NullableTypes>())
                {
                    if (reader.Read())
                        result = reader.Current;
                }

                Assert.IsNotNull(result);

                Assert.IsNull(result.aASCIIString);
                Assert.IsNull(result.aVarcharString);
                Assert.IsNull(result.aVarint);
                Assert.IsNull(result.aTextString);
                Assert.IsNull(result.aBool);
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
        public void SerializeObjectOutNullTest()
        {
            const string insertCql = @"insert into Test.Types(aInt) values (4);";

            const string selectCql = "select * from Test.Types limit 1;";

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                var insertCmd = new CqlCommand(connection, insertCql);
                insertCmd.ExecuteNonQuery();

                var selectCmd = new CqlCommand(connection, selectCql);
                NullableTypes result = null;
                using (var reader = selectCmd.ExecuteReader<NullableTypes>())
                {
                    if (reader.Read())
                        result = reader.Current;
                }

                Assert.IsNotNull(result);

                Assert.IsNull(result.aASCIIString);
                Assert.IsNull(result.aVarcharString);
                Assert.IsNull(result.aVarint);
                Assert.IsNull(result.aTextString);
                Assert.IsNull(result.aBool);
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

        #region Nested type: NullableTypes

        [CqlTable("types")]
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

        // ReSharper restore InconsistentNaming
    }
}