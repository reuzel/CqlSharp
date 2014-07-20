// CqlSharp - CqlSharp.Test
// Copyright (c) 2014 Joost Reuzel
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
using CqlSharp.Serialization;
using CqlSharp.Serialization.Marshal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Test
{
    [TestClass]
    public class CqlTypeTest
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
            const string typeName =
                "org.apache.cassandra.db.marshal.UserType(user_defined_types,61646472657373,737472656574:org.apache.cassandra.db.marshal.UTF8Type,63697479:org.apache.cassandra.db.marshal.UTF8Type,7a69705f636f6465:org.apache.cassandra.db.marshal.Int32Type,70686f6e6573:org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type))";

            var type = CqlType.CreateType(typeName);

            Assert.AreEqual(CqlTypeCode.Custom, type.CqlTypeCode);
            Assert.AreEqual(type.GetType().GetGenericTypeDefinition(), typeof(UserDefinedType<>));
        }

        [TestMethod]
        public void TypeOfUDTClass()
        {
            var type = CqlType.CreateType(typeof(C));

            Assert.IsInstanceOfType(type, typeof(UserDefinedType<C>));

            var udt = (UserDefinedType<C>)type;

            Assert.IsNotNull(udt.Keyspace);
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

            Assert.IsInstanceOfType(type, typeof(UserDefinedType<D>));

            var udt = (UserDefinedType<D>)type;

            Assert.IsNotNull(udt.Keyspace);
            Assert.AreEqual("d", udt.Name);
            Assert.AreEqual(2, udt.GetFieldCount());

            Assert.AreEqual("id", udt.GetFieldName(0));
            Assert.AreEqual(CqlType.Uuid, udt.GetFieldType(0));

            Assert.AreEqual("c", udt.GetFieldName(1));
            Assert.IsInstanceOfType(udt.GetFieldType(1), typeof(UserDefinedType<C>));
            Assert.AreEqual("c", ((UserDefinedType<C>)udt.GetFieldType(1)).Name);
        }

        [TestMethod]
        public void TypeOfUserTypeInList()
        {
            var type = CqlType.CreateType(typeof(List<C>));

            Assert.IsInstanceOfType(type, typeof(ListType<C>));

            var list = (ListType<C>)type;

            Assert.IsInstanceOfType(list.ValueType, typeof(UserDefinedType<C>));
        }

        [TestMethod]
        public void ParseTupleTypeString()
        {
            const string typeName =
                "org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)";

            var type = CqlType.CreateType(typeName);

            Assert.AreEqual(CqlTypeCode.Custom, type.CqlTypeCode);
            Assert.IsInstanceOfType(type, typeof(TupleType<Tuple<string, int>>));
        }

        [TestMethod]
        public void TypeOfTupleInList()
        {
            var type = CqlType.CreateType(typeof(List<Tuple<string, int>>));

            Assert.IsInstanceOfType(type, typeof(ListType<Tuple<string, int>>));

            var list = (ListType<Tuple<string, int>>)type;

            Assert.IsInstanceOfType(list.ValueType, typeof(TupleType<Tuple<string, int>>));
        }

        [TestMethod]
        public void TupleRoundTripSerialization()
        {
            var t = Tuple.Create("hallo", 43);

            var cqlType = CqlType.CreateType(t.GetType());

            byte[] data = cqlType.Serialize(t, 2);

            var result = cqlType.Deserialize<Tuple<string, int>>(data, 2);

            Assert.AreEqual(t.Item1, result.Item1);
            Assert.AreEqual(t.Item2, result.Item2);
        }

        [TestMethod]
        public void TupleRoundTripSerializationWithNullValues()
        {
            var t = Tuple.Create((string)null, 43);

            var cqlType = CqlType.CreateType(t.GetType());

            byte[] data = cqlType.Serialize(t, 2);

            var result = cqlType.Deserialize<Tuple<string, int>>(data, 2);

            Assert.AreEqual(t.Item1, result.Item1);
            Assert.AreEqual(t.Item2, result.Item2);
        }

        #region Nested typeCode: C

#pragma warning disable 0649
#pragma warning disable 169

        [CqlUserType("testudt", "c")]
        private class C
        {
            [CqlKey(IsPartitionKey = true)] [CqlColumn(Order = 1)] public string Id2;

            [CqlKey] [CqlColumn(Order = 0)] public Guid Id;

            [CqlKey(IsPartitionKey = false)] [CqlColumn(Order = 2)] public string Id3;
        }

        [CqlUserType("testudt", "d")]
        private class D
        {
            [CqlKey] [CqlColumn(Order = 0)] public Guid Id;

            [CqlColumn(Order = 1)] public C C;
        }

        #endregion
    }
}