using System;
using System.IO.Packaging;
using CqlSharp.Network.Partition;
using CqlSharp.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Test
{
    [TestClass]
    public class TokenTest
    {
        [TestMethod]
        public void CompositeTokenValue1()
        {
            /* 
             id1 | id2 | token_id1__id2
            -----+-----+----------------------
              10 |  20 | -9026262514124674157
             */

            var key = new PartitionKey();
            key.Set(new[] {CqlType.Int, CqlType.Text}, new object[] {10, "20"});

            var calculatedToken = new MurmurToken();
            calculatedToken.Parse(key.GetValue());

            var tokenFromCassandra = new MurmurToken();
            tokenFromCassandra.Parse("-9026262514124674157");

            Assert.AreEqual(tokenFromCassandra, calculatedToken);
        }

        [TestMethod]
        public void CompositeTokenValueFromClass()
        {
            /* 
             id1 | id2 | token_id1__id2
            -----+-----+----------------------
              10 |  20 | -9026262514124674157
             */

            var key = new PartitionKey();
            key.Set(new CompositeKeyType { Id1 = 10, Id2 = "20", Val="test"});

            var calculatedToken = new MurmurToken();
            calculatedToken.Parse(key.GetValue());

            var tokenFromCassandra = new MurmurToken();
            tokenFromCassandra.Parse("-9026262514124674157");

            Assert.AreEqual(tokenFromCassandra, calculatedToken);
        }

        [TestMethod]
        public void CompositeTokenValue2()
        {
            /* 
             id1 | id2 | token_id1__id2
            -----+-----+----------------------
               1 |   2 |  4093852821103061060
             */

            var key = new PartitionKey();
            key.Set(new[] { CqlType.Int, CqlType.Text }, new object[] { 1, "2" });

            var calculatedToken = new MurmurToken();
            calculatedToken.Parse(key.GetValue());

            var tokenFromCassandra = new MurmurToken();
            tokenFromCassandra.Parse("4093852821103061060");

            Assert.AreEqual(tokenFromCassandra, calculatedToken);
        }

        [TestMethod]
        public void SingleTokenValue()
        {
            /* 
             id1 | token_id1
            -----+----------------------
               1 | -4069959284402364209
             */

            var key = new PartitionKey();
            key.Set(CqlType.Int, 1);

            var calculatedToken = new MurmurToken();
            calculatedToken.Parse(key.GetValue());

            var tokenFromCassandra = new MurmurToken();
            tokenFromCassandra.Parse("-4069959284402364209");

            Assert.AreEqual(tokenFromCassandra, calculatedToken);
        }

        [TestMethod]
        public void SingleTokenValueFromArray()
        {
            /* 
             id1 | token_id1
            -----+----------------------
               1 | -4069959284402364209
             */

            var key = new PartitionKey();
            key.Set(new[] { CqlType.Int }, new object[] { 1 });

            var calculatedToken = new MurmurToken();
            calculatedToken.Parse(key.GetValue());

            var tokenFromCassandra = new MurmurToken();
            tokenFromCassandra.Parse("-4069959284402364209");

            Assert.AreEqual(tokenFromCassandra, calculatedToken);
        }
        
        class CompositeKeyType
        {
            [CqlColumn(Order = 0)]
            [CqlKey(IsPartitionKey = true)]
            public int Id1 { get; set; }

            [CqlColumn(Order = 1)]
            [CqlKey(IsPartitionKey = true)]
            public string Id2 { get; set; }

            public string Val { get; set; }
        }
    }
}
