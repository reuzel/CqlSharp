// CqlSharp.Linq - CqlSharp.Linq.Test
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

using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;

namespace CqlSharp.Linq.Test
{
    [TestClass]
    public class MutationTest
    {

        [TestMethod]
        public void AddTwice()
        {
            using (var context = new MyContext())
            {
                var value = new MyValue { Id = 1, Value = "1" };

                Assert.IsTrue(context.Values.Add(value));
                Assert.AreEqual(1, context.Values.Local.Count());

                Assert.IsFalse(context.Values.Add(value));
                Assert.AreEqual(1, context.Values.Local.Count());

            }
        }

        [TestMethod]
        public void AddRange()
        {
            using (var context = new MyContext())
            {
                var values = new[]
                                 {
                                     new MyValue { Id = 1, Value = "1" },
                                     new MyValue { Id = 2, Value = "2" },
                                     new MyValue { Id = 3, Value = "3" },
                                     new MyValue { Id = 4, Value = "4" }
                                 };

                //add and check if there
                context.Values.AddRange(values);
                Assert.AreEqual(4, context.ChangeTracker.Entries().Count());

                //add again, and check if no changes...
                context.Values.AddRange(values);
                Assert.AreEqual(4, context.ChangeTracker.Entries().Count());

            }
        }

        [TestMethod]
        public void AddWithExistingKey()
        {
            using (var context = new MyContext())
            {
                var value = new MyValue { Id = 1, Value = "1" };
                context.Values.Add(value);

                var value2 = new MyValue { Id = 1, Value = "2" };
                Assert.IsFalse(context.Values.Add(value2));
            }
        }

        [TestMethod]
        public void DeleteNonTracked()
        {
            using (var context = new MyContext())
            {
                var value = new MyValue { Id = 1, Value = "1" };

                Assert.IsTrue(context.Values.Delete(value));
                Assert.AreEqual(1, context.Values.Local.Count());

                Assert.IsFalse(context.Values.Add(value));
                Assert.AreEqual(1, context.Values.Local.Count());

            }
        }

        [TestMethod]
        [ExpectedException(typeof(CqlLinqException))]
        public void ChangeKeyResultsInException()
        {
            using (var context = new MyContext())
            {
                context.ChangeTracker.AutoDetectChangesEnabled = false;

                var value = new MyValue { Id = 1, Value = "1" };
                Assert.IsTrue(context.Values.Add(value));
                Assert.IsTrue(context.ChangeTracker.HasChanges());

                value.Id = 2;

                //next should throw exception
                context.ChangeTracker.DetectChanges();
            }
        }

        [TestMethod]
        public void AddThenAttach()
        {
            using (var context = new MyContext())
            {
                var value = new MyValue { Id = 1, Value = "1" };
                context.Values.Add(value);

                Assert.IsFalse(context.Values.Attach(value));
            }
        }

        [TestMethod]
        public void AttachDetach()
        {
            using (var context = new MyContext())
            {
                var value = new MyValue { Id = 1, Value = "1" };
                Assert.IsTrue(context.Values.Attach(value));
                Assert.IsFalse(context.Values.Attach(value));
                Assert.IsTrue(context.Values.Detach(value));
                Assert.IsFalse(context.Values.Detach(value));

                Assert.IsFalse(context.ChangeTracker.HasChanges());
                Assert.AreEqual(0, context.Values.Local.Count());
            }
        }

        [TestMethod]
        public void NoTrackingContextAddAttachDeleteDetachEntity()
        {
            using (var context = new MyContext())
            {
                context.TrackChanges = false;

                var value = new MyValue { Id = 1, Value = "1" };
                Assert.IsFalse(context.Values.Attach(value));
                Assert.IsFalse(context.Values.Add(value));
                Assert.IsFalse(context.Values.Delete(value));
                Assert.IsFalse(context.Values.Detach(value));

                Assert.IsFalse(context.ChangeTracker.HasChanges());
                Assert.AreEqual(0, context.Values.Local.Count());
            }
        }

        [TestMethod]
        public void ContextDisableTrackingClearsTrackedEntries()
        {
            using (var context = new MyContext())
            {

                var value = new MyValue { Id = 1, Value = "1" };
                Assert.IsTrue(context.Values.Add(value));
                Assert.IsTrue(context.ChangeTracker.HasChanges());

                context.TrackChanges = false;

                Assert.IsFalse(context.ChangeTracker.HasChanges());
                Assert.AreEqual(0, context.ChangeTracker.Entries<MyValue>().Count());
                Assert.AreEqual(0, context.ChangeTracker.Entries().Count());
            }
        }
    }
}