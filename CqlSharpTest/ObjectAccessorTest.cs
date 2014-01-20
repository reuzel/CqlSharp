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
            Assert.IsTrue(accessor.IsTableSet);
            Assert.AreEqual("a", accessor.Table);

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
                        Assert.AreEqual(CqlType.Int, info.CqlType);
                        break;
                    case "value":
                        Assert.AreEqual(CqlType.Varchar, info.CqlType);
                        break;
                    case "entries":
                        Assert.AreEqual(CqlType.Map, info.CqlType);
                        break;
                    case "readonlyentries":
                        Assert.AreEqual(CqlType.List, info.CqlType);
                        break;
                    case "writeonlyentries":
                        Assert.AreEqual(CqlType.Set, info.CqlType);
                        break;
                    case "constant":
                        Assert.AreEqual(CqlType.Bigint, info.CqlType);
                        break;
                    default:
                        Assert.Fail("unknown member name {0} found!", info.Name);
                        break;
                }

                switch (info.Name)
                {
                    case "readonlyentries":
                    case "constant":
                        Assert.IsNotNull(info.ReadFunction);
                        Assert.IsNull(info.WriteFunction);
                        break;

                    case "writeonlyentries":
                        Assert.IsNull(info.ReadFunction);
                        Assert.IsNotNull(info.WriteFunction);
                        break;
                    default:
                        Assert.IsNotNull(info.ReadFunction);
                        Assert.IsNotNull(info.WriteFunction);
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
            Assert.IsTrue(accessor.IsTableSet);
            Assert.AreEqual("bTable", accessor.Table);

            Assert.AreEqual(2, accessor.Columns.Count);
            Assert.AreEqual(6, accessor.ColumnsByName.Count);
            Assert.AreEqual(1, accessor.PartitionKeys.Count);
            Assert.AreEqual(0, accessor.ClusteringKeys.Count);
            Assert.AreEqual(1, accessor.NormalColumns.Count);

            MemberInfo member = typeof(B).GetField("Id");
            var info = accessor.ColumnsByMember[member];
            Assert.AreEqual("guid", info.Name);
            Assert.AreEqual(CqlType.Timeuuid, info.CqlType);
            Assert.IsTrue(info.Order.HasValue);
            Assert.AreEqual(0, info.Order.Value);
            Assert.IsTrue(info.IsPartitionKey);
            Assert.IsFalse(info.IsClusteringKey);
            Assert.IsFalse(info.IsIndexed);
            Assert.IsNull(info.IndexName);
            Assert.AreEqual(member, info.MemberInfo);
            Assert.IsNotNull(info.ReadFunction);
            Assert.IsNotNull(info.WriteFunction);

            member = typeof(B).GetProperty("Indexed");
            info = accessor.ColumnsByMember[member];
            Assert.AreEqual("index", info.Name);
            Assert.AreEqual(CqlType.Varchar, info.CqlType);
            Assert.IsFalse(info.Order.HasValue);
            Assert.IsFalse(info.IsPartitionKey);
            Assert.IsFalse(info.IsClusteringKey);
            Assert.IsTrue(info.IsIndexed);
            Assert.AreEqual("bTableIndex", info.IndexName);
            Assert.AreEqual(member, info.MemberInfo);
            Assert.IsNotNull(info.ReadFunction);
            Assert.IsNotNull(info.WriteFunction);
        }

        [TestMethod]
        public void CheckClassWithComplexKey()
        {
            var accessor = ObjectAccessor<C>.Instance;

            MemberInfo member = typeof(C).GetField("Id");
            var info = accessor.ColumnsByMember[member];
            Assert.IsTrue(info.Order.HasValue);
            Assert.AreEqual(0, info.Order.Value);
            Assert.IsTrue(info.IsPartitionKey);
            Assert.IsFalse(info.IsClusteringKey);

            member = typeof(C).GetField("Id2");
            info = accessor.ColumnsByMember[member];
            Assert.IsTrue(info.Order.HasValue);
            Assert.AreEqual(1, info.Order.Value);
            Assert.IsTrue(info.IsPartitionKey);
            Assert.IsFalse(info.IsClusteringKey);

            member = typeof(C).GetField("Id3");
            info = accessor.ColumnsByMember[member];
            Assert.IsTrue(info.Order.HasValue);
            Assert.AreEqual(2, info.Order.Value);
            Assert.IsFalse(info.IsPartitionKey);
            Assert.IsTrue(info.IsClusteringKey);
        }

        #region Nested type: A

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

        #region Nested type: B

        [CqlTable("bTable", Keyspace = "bKeyspace")]
        private class B
        {
            [CqlKey]
            [CqlColumn("guid", CqlType.Timeuuid)]
            public Guid Id;

            [CqlIndex(Name = "bTableIndex")]
            [CqlColumn("index")]
            public string Indexed { get; set; }

            [CqlIgnore]
            public long Ignored { get; set; }
        }

        #endregion

        #region Nested type: C

        private class C
        {
            [CqlKey(IsPartitionKey = true, Order = 0)]
            public Guid Id;

            [CqlKey(IsPartitionKey = true, Order = 1)]
            public string Id2;
            [CqlKey(IsPartitionKey = false, Order = 2)]
            public string Id3;
        }

        #endregion
    }
}