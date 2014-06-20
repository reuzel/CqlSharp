using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using CqlSharp.Serialization;
using CqlSharp.Serialization.Marshal;

namespace CqlSharp.Test
{
    [TestClass]
    public class TypeParserTest
    {
        [TestMethod]
        public void ParseSimpleAscii()
        {
            const string typeName = "AsciiType";

            CqlType type = CqlType.CreateType(typeName);

            Assert.AreEqual(CqlType.Ascii, type);
        }

        [TestMethod]
        public void ParseAsciiList()
        {
            const string typeName = "ListType(AsciiType)";

            CqlType type = CqlType.CreateType(typeName);

            Assert.AreEqual(CqlType.CreateType(CqlTypeCode.List, CqlType.Ascii), type);
        }

        [TestMethod]
        public void ParseUUIDVarcharMap()
        {
            const string typeName = "MapType(UUIDType,UTF8Type)";
            CqlType expected = CqlType.CreateType(CqlTypeCode.Map, CqlType.Uuid, CqlType.Varchar);

            CqlType type = CqlType.CreateType(typeName);
            
            Assert.AreEqual(expected, type);
        }

        [TestMethod]
        public void CreateDictionaryType()
        {
            
            CqlType expected = CqlType.CreateType(CqlTypeCode.Map, CqlType.Uuid, CqlType.Varchar);

            CqlType type = CqlType.CreateType(typeof(Dictionary<Guid, string>));

            Assert.AreEqual(expected, type);
        }

        [TestMethod]
        public void ParseUDTTypeString()
        {
            const string typeName = "org.apache.cassandra.db.marshal.UserType(user_defined_types,61646472657373,737472656574:org.apache.cassandra.db.marshal.UTF8Type,63697479:org.apache.cassandra.db.marshal.UTF8Type,7a69705f636f6465:org.apache.cassandra.db.marshal.Int32Type,70686f6e6573:org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type))";

            var type = CqlType.CreateType(typeName);

            Assert.AreEqual(CqlTypeCode.Custom, type.CqlTypeCode);
            Assert.IsInstanceOfType(type, typeof(UserDefinedType));
        }

        [TestMethod]
        public void TypeOfUDTClass()
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

        [TestMethod]
        public void TypeOfNestedUDTTypes()
        {
            var type = CqlType.CreateType(typeof(D));

            Assert.IsInstanceOfType(type, typeof(UserDefinedType));

            var udt = (UserDefinedType)type;

            Assert.IsNull(udt.Keyspace);
            Assert.AreEqual("d", udt.Name);
            Assert.AreEqual(2, udt.GetFieldCount());

            Assert.AreEqual("id", udt.GetFieldName(0));
            Assert.AreEqual(CqlType.Uuid, udt.GetFieldType(0));

            Assert.AreEqual("c", udt.GetFieldName(1));
            Assert.IsInstanceOfType(udt.GetFieldType(1), typeof(UserDefinedType));
            Assert.AreEqual("c", ((UserDefinedType)udt.GetFieldType(1)).Name);
        }

        [TestMethod]
        public void TypeOfUserTypeInList()
        {
            var type = CqlType.CreateType(typeof(List<C>));

            Assert.IsInstanceOfType(type, typeof(ListType<UserDefined>));

            var list = (ListType<UserDefined>)type;

            Assert.IsInstanceOfType(list.ValueType, typeof(UserDefinedType));
        }

#pragma warning disable 0649

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

        [CqlUserType]
        private class D
        {
            [CqlKey]
            [CqlColumn(Order = 0)]
            public Guid Id;

            [CqlColumn(Order = 1)]
            public C c;
        }
        #endregion


    }
}
