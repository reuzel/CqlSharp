using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqlSharp;
using CqlSharp.Serialization.Marshal;
using CqlSharp.Serialization;

namespace CqlSharp.Test
{
    [TestClass]
    public class UserDefinedTypeTest
    {
        [TestMethod]
        public void ParseUDT()
        {
            const string typeName = "org.apache.cassandra.db.marshal.UserType(user_defined_types,61646472657373,737472656574:org.apache.cassandra.db.marshal.UTF8Type,63697479:org.apache.cassandra.db.marshal.UTF8Type,7a69705f636f6465:org.apache.cassandra.db.marshal.Int32Type,70686f6e6573:org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type))";

            var type = CqlType.CreateType(typeName);

            Assert.AreEqual(CqlTypeCode.Custom, type.CqlTypeCode);
            Assert.IsInstanceOfType(type, typeof(UserDefinedType));
        }

        [TestMethod]
        public void TypeForClass()
        {
            var type = CqlType.CreateType(typeof(C));

            Assert.IsInstanceOfType(type, typeof(UserDefinedType));

            var udt = (UserDefinedType)type;

            Assert.IsNull(udt.Keyspace);
            Assert.AreEqual("c", udt.Name);
            Assert.AreEqual(3, udt.GetFieldCount());
            
            Assert.AreEqual("id", udt.GetFieldName(0));
            Assert.AreEqual(CqlType.Uuid, udt.GetFieldType(0));

            Assert.AreEqual("id2", udt.GetFieldName(1));
            Assert.AreEqual(CqlType.Varchar, udt.GetFieldType(1));

            Assert.AreEqual("id3", udt.GetFieldName(2));
            Assert.AreEqual(CqlType.Varchar, udt.GetFieldType(2));
        }

        #region Nested typeCode: C

        [CqlUserType]
        private class C
        {
            [CqlKey(IsPartitionKey = true)]
            [CqlColumn(Order = 1)]
            public string Id2;

            [CqlKey]
            [CqlColumn(Order = 0)]
            public Guid Id;

            [CqlKey(IsPartitionKey = false)]
            [CqlColumn(Order = 2)]
            public string Id3;
        }

        #endregion
    }
}
