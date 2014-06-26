using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Test
{
    [TestClass]
    public class TupleTypeTest
    {
        [TestMethod]
        public void RoundTripSerialization()
        {
            var t = Tuple.Create("hallo", 43);

            var cqlType = CqlType.CreateType(t.GetType());

            byte[] data = cqlType.Serialize(t);

            var result = cqlType.Deserialize<Tuple<string, int>>(data);

            Assert.AreEqual(t.Item1, result.Item1);
            Assert.AreEqual(t.Item2, result.Item2);

        }

        [TestMethod]
        public void RoundTripSerializationWithNullValues()
        {
            var t = Tuple.Create((string)null, 43);

            var cqlType = CqlType.CreateType(t.GetType());

            byte[] data = cqlType.Serialize(t);

            var result = cqlType.Deserialize<Tuple<string, int>>(data);

            Assert.AreEqual(t.Item1, result.Item1);
            Assert.AreEqual(t.Item2, result.Item2);

        }
    }
}
