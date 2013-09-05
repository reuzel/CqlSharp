// CqlSharp - CqlSharp.Test
// Copyright (c) 2013 Joost Reuzel
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

using System.Collections.Generic;
using CqlSharp.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Test
{
    [TestClass]
    public class CqlParameterTest
    {
        [TestMethod]
        public void DeriveTypeFromStringValue()
        {
            var param = new CqlParameter("hello.world", "hello2");

            Assert.IsNull(param.Keyspace);
            Assert.AreEqual("hello", param.Table);
            Assert.AreEqual("world", param.ColumnName);
            Assert.AreEqual("hello.world", param.ParameterName);
            Assert.AreEqual("hello2", param.Value);
            Assert.AreEqual(CqlType.Varchar, param.CqlType);
        }

        [TestMethod]
        public void DeriveTypeFromMapValue()
        {
            var param = new CqlParameter("say.hello.world.me", new Dictionary<string, int> {{"hi", 1}, {"there", 2}});

            Assert.AreEqual("say", param.Keyspace);
            Assert.AreEqual("hello", param.Table);
            Assert.AreEqual("world.me", param.ColumnName);
            Assert.AreEqual("say.hello.world.me", param.ParameterName);
            Assert.AreEqual(CqlType.Map, param.CqlType);
            Assert.AreEqual(CqlType.Varchar, param.CollectionKeyType);
            Assert.AreEqual(CqlType.Int, param.CollectionValueType);
        }

        [TestMethod]
        public void SetParametersFromObject()
        {
            var collection = new CqlParameterCollection();
            collection.Add("dummy.test.id", CqlType.Int);
            collection.Add("dummy.test.value", CqlType.Text);
            collection.Add("dummy.test.ignored", CqlType.Blob);

            var a = new A {Id = 1, Value2 = new byte[] {1, 2}, Value = "Hello!"};

            collection.Set(a);

            Assert.AreEqual(a.Id, collection["dummy.test.id"].Value);
            Assert.AreEqual(a.Value, collection["dummy.test.value"].Value);
            Assert.IsNull(collection["dummy.test.ignored"].Value);
        }

        [TestMethod]
        public void SetNameOnlyParametersFromObject()
        {
            var collection = new CqlParameterCollection();
            collection.Add("id", CqlType.Int);
            collection.Add("value", CqlType.Text);
            collection.Add("ignored", CqlType.Blob);

            var a = new A {Id = 1, Value2 = new byte[] {1, 2}, Value = "Hello!"};

            collection.Set(a);

            Assert.AreEqual(a.Id, collection["id"].Value);
            Assert.AreEqual(a.Value, collection["value"].Value);
            Assert.IsNull(collection["ignored"].Value);
        }

        [TestMethod]
        public void SetParametersFromTwoObjects()
        {
            var collection = new CqlParameterCollection();
            collection.Add("test.id", CqlType.Int);
            collection.Add("test.value", CqlType.Text);
            collection.Add("test.value2", CqlType.Blob);
            collection.Add("test2.id", CqlType.Int);
            collection.Add("test2.value", CqlType.Text);
            collection.Add("test2.value2", CqlType.Blob);
            collection.Fixate();

            var a = new A {Id = 1, Value2 = new byte[] {1, 2}, Value = "Hello!"};
            var b = new B {Id = 2, Value2 = new byte[] {3, 4}, Value = "World!"};

            collection.Set(a);
            collection.Set(b);

            Assert.AreEqual(a.Id, collection["test.id"].Value);
            Assert.AreEqual(a.Value, collection["test.value"].Value);
            Assert.IsNull(collection["test.value2"].Value);
            Assert.AreEqual(b.Id, collection["test2.id"].Value);
            Assert.AreEqual(b.Value, collection["test2.value"].Value);
            Assert.AreEqual(b.Value2, collection["test2.value2"].Value);
        }

        [TestMethod]
        public void SetParametersFromAnonymousObject()
        {
            var collection = new CqlParameterCollection
                                 {
                                     {"test.id", CqlType.Int},
                                     {"test.value", CqlType.Text},
                                     {"test.value2", CqlType.Blob},
                                     {"test.Map", CqlType.Map, CqlType.Text, CqlType.Boolean}
                                 };
            collection.Fixate();

            var a = new {Id = 1, Value2 = new byte[] {1, 2}, Value = "Hello!"};

            collection.Set(a);

            Assert.AreEqual(a.Id, collection["test.id"].Value);
            Assert.AreEqual(a.Value, collection["test.value"].Value);
            Assert.AreEqual(a.Value2, collection["test.value2"].Value);
            Assert.IsNull(collection[3].Value);
        }

        #region Nested type: A

        [CqlTable("test")]
        private class A
        {
            public int Id { get; set; }
            public string Value { get; set; }

            [CqlIgnore]
// ReSharper disable UnusedAutoPropertyAccessor.Local
                public byte[] Value2 { get; set; }
// ReSharper restore UnusedAutoPropertyAccessor.Local
        }

        #endregion

        #region Nested type: B

        [CqlTable("test2")]
        private class B
        {
            public int Id { get; set; }
            public string Value { get; set; }
            public byte[] Value2 { get; set; }
        }

        #endregion
    }
}