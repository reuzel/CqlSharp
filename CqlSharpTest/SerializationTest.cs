using CqlSharp;
using CqlSharp.Protocol.Exceptions;
using CqlSharp.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Numerics;

namespace CqlSharpTest
{
    [TestClass]
    public class SerializationTest
    {
        private const string ConnectionString = "server=localhost;throttle=100;ConnectionStrategy=PartitionAware";

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

        public class Types
        {
            [CqlColumnAttribute("aint", CqlType = CqlType.Int, PartitionKeyIndex = 0)]
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

        [TestInitialize]
        public void Init()
        {
            using (var connection = new CqlConnection(ConnectionString))
            {
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

                var values = new Types()
                                 {
                                     aASCIIString = "hello world!",
                                     aBlob = new byte[] { 1, 2, 3, 4 },
                                     aBool = true,
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
                                     aList = new List<string>() { "string 1", "string 2" },
                                     aSet = new HashSet<int>() { 1, 3, 3 },
                                     aMap =
                                         new Dictionary<long, string>() { { 1, "value 1" }, { 2, "value 2" }, { 3, "value 3" } },
                                 };

                var insertCmd = new CqlCommand(connection, insertCql);
                insertCmd.Prepare();
                insertCmd.Parameters.Set(values);
                insertCmd.PartitionKey.Set(values);
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
    }
}
