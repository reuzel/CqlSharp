using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace CqlSharp.Test
{
    [TestClass]
    public class CqlParameterTest
    {

        [TestMethod]
        public void DeriveTypeFromStringValue()
        {
            var param = new CqlParameter("hello.world", "hello2");

            Assert.AreEqual("hello", param.Table);
            Assert.AreEqual("world", param.ColumnName);
            Assert.AreEqual("hello.world", param.ParameterName);
            Assert.AreEqual("hello2", param.Value);
            Assert.AreEqual(CqlType.Varchar, param.CqlType);
        }

        [TestMethod]
        public void DeriveTypeFromMapValue()
        {
            var param = new CqlParameter("say.hello.world.me", new Dictionary<string, int> { { "hi", 1 }, { "there", 2 } });

            Assert.AreEqual("say", param.Keyspace);
            Assert.AreEqual("hello", param.Table);
            Assert.AreEqual("world.me", param.ColumnName);
            Assert.AreEqual("say.hello.world.me", param.ParameterName);
            Assert.AreEqual(CqlType.Map, param.CqlType);
            Assert.AreEqual(CqlType.Varchar, param.CollectionKeyType);
            Assert.AreEqual(CqlType.Int, param.CollectionValueType);
        }
    }
}
