using System;
using CqlSharp.Linq.Mutations;
using CqlSharp.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Linq.Test
{
    [TestClass]
    public class EntityHelperTest
    {
        class MyEntity
        {
            [CqlKey(IsPartitionKey = true)]
            [CqlColumn(Order = 0)]
            public int Id;

            [CqlKey(IsPartitionKey = false)]
            [CqlColumn(Order = 1)]
            public Guid Cluster { get; set; }

            public string Value { get; set; }

            [CqlIgnore]
            public string Ignorable { get; set; }
        }

        [TestMethod]
        public void CloneTest()
        {
            var source = new MyEntity
                             {
                                 Id = 1,
                                 Cluster = Guid.NewGuid(),
                                 Value = "Hello World!",
                                 Ignorable = "not me!"
                             };

            var target = EntityHelper<MyEntity>.Instance.Clone(source);

            Assert.IsNotNull(target);
            Assert.AreNotSame(source, target);
            Assert.AreEqual(source.Id, target.Id);
            Assert.AreEqual(source.Cluster, target.Cluster);
            Assert.AreEqual(source.Value, target.Value);
            Assert.IsNull(target.Ignorable);
        }

        [TestMethod]
        public void CloneKeyTest()
        {
            var source = new MyEntity
            {
                Id = 1,
                Cluster = Guid.NewGuid(),
                Value = "Hello World!",
                Ignorable = "not me!"
            };

            var target = EntityHelper<MyEntity>.Instance.CloneKey(source);

            Assert.IsNotNull(target);
            Assert.AreNotSame(source, target);
            Assert.AreEqual(source.Id, target.Id);
            Assert.AreEqual(source.Cluster, target.Cluster);
            Assert.IsNull(target.Value);
            Assert.IsNull(target.Ignorable);
        }

        [TestMethod]
        public void CopyToTest()
        {
            var source = new MyEntity
            {
                Id = 1,
                Cluster = Guid.NewGuid(),
                Value = "Hello World!",
                Ignorable = "not me!"
            };

            var target = new MyEntity();

            EntityHelper<MyEntity>.Instance.CopyTo(source, target);

            Assert.AreEqual(source.Id, target.Id);
            Assert.AreEqual(source.Cluster, target.Cluster);
            Assert.AreEqual(source.Value, target.Value);
            Assert.IsNull(target.Ignorable);
        }

        [TestMethod]
        public void HashCodeTest()
        {
            var source = new MyEntity
            {
                Id = 1,
                Cluster = Guid.NewGuid(),
                Value = "Hello World!",
                Ignorable = "not me!"
            };

            var expected = (17*23+1.GetHashCode())*23 + source.Cluster.GetHashCode();

            var actual = EntityHelper<MyEntity>.Instance.GetHashCode(source);

            Assert.AreEqual(expected, actual);
        }

    }
}
