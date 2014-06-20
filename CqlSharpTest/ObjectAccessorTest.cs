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

using CqlSharp.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

#pragma warning disable 0168
#pragma warning disable 0169
#pragma warning disable 0649
// ReSharper disable UnusedMember.Local

namespace CqlSharp.Test
{
    [TestClass]
    public class ObjectAccessorTest
    {
        [TestMethod]
        public void CheckClassWithNoAttributes()
        {
            var accessor = ObjectAccessor<A>.Instance;

            Assert.IsFalse(accessor.IsKeySpaceSet);
            Assert.IsNull(accessor.Keyspace);
            Assert.IsTrue(accessor.IsNameSet);
            Assert.AreEqual("a", accessor.Name);

            Assert.AreEqual(6, accessor.Columns.Count);
            Assert.AreEqual(12, accessor.ColumnsByName.Count);
            Assert.AreEqual(0, accessor.PartitionKeys.Count);
            Assert.AreEqual(0, accessor.ClusteringKeys.Count);
            Assert.AreEqual(6, accessor.NormalColumns.Count);

            var members = typeof(A).GetProperties().Union((IEnumerable<MemberInfo>)typeof(A).GetFields());

            foreach (var member in members)
            {
                var info = accessor.ColumnsByMember[member];
                Assert.AreEqual(member.Name.ToLower(), info.Name);
                Assert.IsNull(info.Order);
                Assert.IsFalse(info.IsPartitionKey);
                Assert.IsFalse(info.IsClusteringKey);
                Assert.IsFalse(info.IsIndexed);
                Assert.IsNull(info.IndexName);
                Assert.AreEqual(member, info.MemberInfo);

                switch (info.Name)
                {
                    case "id":
                        Assert.AreEqual(CqlTypeCode.Int, info.CqlType.CqlTypeCode);
                        break;
                    case "value":
                        Assert.AreEqual(CqlTypeCode.Varchar, info.CqlType.CqlTypeCode);
                        break;
                    case "entries":
                        Assert.AreEqual(CqlTypeCode.Map, info.CqlType.CqlTypeCode);
                        break;
                    case "readonlyentries":
                        Assert.AreEqual(CqlTypeCode.List, info.CqlType.CqlTypeCode);
                        break;
                    case "writeonlyentries":
                        Assert.AreEqual(CqlTypeCode.Set, info.CqlType.CqlTypeCode);
                        break;
                    case "constant":
                        Assert.AreEqual(CqlTypeCode.Bigint, info.CqlType.CqlTypeCode);
                        break;
                    default:
                        Assert.Fail("unknown member name {0} found!", info.Name);
                        break;
                }
            }
        }

        [TestMethod]
        public void CheckClassWithAttributes()
        {
            var accessor = ObjectAccessor<B>.Instance;

            Assert.IsTrue(accessor.IsKeySpaceSet);
            Assert.AreEqual("bKeyspace", accessor.Keyspace);
            Assert.IsTrue(accessor.IsNameSet);
            Assert.AreEqual("bTable", accessor.Name);

            Assert.AreEqual(2, accessor.Columns.Count);
            Assert.AreEqual(6, accessor.ColumnsByName.Count);
            Assert.AreEqual(1, accessor.PartitionKeys.Count);
            Assert.AreEqual(0, accessor.ClusteringKeys.Count);
            Assert.AreEqual(1, accessor.NormalColumns.Count);

            MemberInfo member = typeof(B).GetField("Id");
            var info = accessor.ColumnsByMember[member];
            Assert.AreEqual("guid", info.Name);
            Assert.AreEqual(CqlTypeCode.Timeuuid, info.CqlType.CqlTypeCode);
            Assert.IsFalse(info.Order.HasValue);
            Assert.IsTrue(info.IsPartitionKey);
            Assert.IsFalse(info.IsClusteringKey);
            Assert.IsFalse(info.IsIndexed);
            Assert.IsNull(info.IndexName);
            Assert.AreEqual(member, info.MemberInfo);

            member = typeof(B).GetProperty("Indexed");
            info = accessor.ColumnsByMember[member];
            Assert.AreEqual("index", info.Name);
            Assert.AreEqual(CqlTypeCode.Varchar, info.CqlType.CqlTypeCode);
            Assert.IsFalse(info.Order.HasValue);
            Assert.IsFalse(info.IsPartitionKey);
            Assert.IsFalse(info.IsClusteringKey);
            Assert.IsTrue(info.IsIndexed);
            Assert.AreEqual("bTableIndex", info.IndexName);
            Assert.AreEqual(member, info.MemberInfo);
        }

        [TestMethod]
        public void CheckClassWithComplexKey()
        {
            var accessor = ObjectAccessor<C>.Instance;

            MemberInfo member = typeof(C).GetField("Id");
            var info1 = accessor.ColumnsByMember[member];
            Assert.IsTrue(info1.Order.HasValue);
            Assert.AreEqual(0, info1.Order.Value);
            Assert.IsTrue(info1.IsPartitionKey);
            Assert.IsFalse(info1.IsClusteringKey);

            member = typeof(C).GetField("Id2");
            var info2 = accessor.ColumnsByMember[member];
            Assert.IsTrue(info2.Order.HasValue);
            Assert.AreEqual(1, info2.Order.Value);
            Assert.IsTrue(info2.IsPartitionKey);
            Assert.IsFalse(info2.IsClusteringKey);

            member = typeof(C).GetField("Id3");
            var info3 = accessor.ColumnsByMember[member];
            Assert.IsTrue(info3.Order.HasValue);
            Assert.AreEqual(2, info3.Order.Value);
            Assert.IsFalse(info3.IsPartitionKey);
            Assert.IsTrue(info3.IsClusteringKey);

            Assert.AreEqual(2, accessor.PartitionKeys.Count);
            Assert.AreEqual(info1, accessor.PartitionKeys[0]);
            Assert.AreEqual(info2, accessor.PartitionKeys[1]);

            Assert.AreEqual(1, accessor.ClusteringKeys.Count);
            Assert.AreEqual(info3, accessor.ClusteringKeys[0]);

        }

        #region Nested typeCode: A

        private class A
        {
            public readonly long Constant = 0;
            public int Id;

            public string Value { get; set; }

            public Dictionary<string, Guid> Entries { get; set; }

            public List<string> ReadOnlyEntries
            {
                get { return null; }
            }

            public HashSet<string> WriteOnlyEntries
            {
                set { var dummy = value; }
            }
        }

        #endregion

        #region Nested typeCode: B

        [CqlTable("bTable", Keyspace = "bKeyspace")]
        private class B
        {
            [CqlKey]
            [CqlColumn("guid", CqlTypeCode.Timeuuid)]
            public Guid Id;

            [CqlIndex(Name = "bTableIndex")]
            [CqlColumn("index")]
            public string Indexed { get; set; }

            [CqlIgnore]
            public long Ignored { get; set; }
        }

        #endregion

        #region Nested typeCode: C

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