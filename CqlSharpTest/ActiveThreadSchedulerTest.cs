// SingleThreadScheduler - SingleThreadScheduler.Test
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
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Threading;
using CqlSharp.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Test
{
    [TestClass]
    public class ActiveThreadSchedulerTest
    {
        public static int ThreadId()
        {
            return Thread.CurrentThread.ManagedThreadId;
        }

        [TestMethod]
        public void Yield()
        {
            SyncContextHelper.Invoke(() =>
            {

                var threadId = ThreadId();
                var scheduler = TaskScheduler.Current;
                var context = SynchronizationContext.Current;

                Scheduler.RunSynchronously(async () =>
                {
                    Assert.IsNull(SynchronizationContext.Current);
                    Assert.IsInstanceOfType(TaskScheduler.Current, typeof(ActiveThreadScheduler));
                    Assert.AreEqual(threadId, ThreadId());

                    await Task.Yield();

                    Assert.IsNull(SynchronizationContext.Current);
                    Assert.IsInstanceOfType(TaskScheduler.Current, typeof(ActiveThreadScheduler));
                    Assert.AreEqual(threadId, ThreadId());
                });

                Assert.AreEqual(context, SynchronizationContext.Current);
                Assert.AreEqual(scheduler, TaskScheduler.Current);
                Assert.AreEqual(threadId, ThreadId());
            });
        }


        [TestMethod]
        public void CompletedTask()
        {
            SyncContextHelper.Invoke(() =>
            {

                var threadId = ThreadId();
                var scheduler = TaskScheduler.Current;
                var context = SynchronizationContext.Current;

                Scheduler.RunSynchronously(async () =>
                {
                    Assert.IsNull(SynchronizationContext.Current);
                    Assert.IsInstanceOfType(TaskScheduler.Current, typeof(ActiveThreadScheduler));
                    Assert.AreEqual(threadId, ThreadId());

                    await Task.FromResult(true).AutoConfigureAwait();

                    Assert.IsNull(SynchronizationContext.Current);
                    Assert.IsInstanceOfType(TaskScheduler.Current, typeof(ActiveThreadScheduler));
                    Assert.AreEqual(threadId, ThreadId());
                });

                Assert.AreEqual(context, SynchronizationContext.Current);
                Assert.AreEqual(scheduler, TaskScheduler.Current);
                Assert.AreEqual(threadId, ThreadId());
            });
        }

        [TestMethod]
        public void Delay()
        {
            SyncContextHelper.Invoke(() =>
            {

                var threadId = ThreadId();
                var scheduler = TaskScheduler.Current;
                var context = SynchronizationContext.Current;

                Scheduler.RunSynchronously(async () =>
                {
                    Assert.IsNull(SynchronizationContext.Current);
                    Assert.IsInstanceOfType(TaskScheduler.Current, typeof(ActiveThreadScheduler));
                    Assert.AreEqual(threadId, ThreadId());

                    await Task.Delay(10).AutoConfigureAwait();

                    Assert.IsNull(SynchronizationContext.Current);
                    Assert.IsInstanceOfType(TaskScheduler.Current, typeof(ActiveThreadScheduler));
                    Assert.AreEqual(threadId, ThreadId());
                });

                Assert.AreEqual(context, SynchronizationContext.Current);
                Assert.AreEqual(scheduler, TaskScheduler.Current);
                Assert.AreEqual(threadId, ThreadId());
            });
        }

        [TestMethod]
        public void DelayIndirect()
        {
           SyncContextHelper.Invoke(() =>
            {

                var threadId = ThreadId();
                var scheduler = TaskScheduler.Current;
                var context = SynchronizationContext.Current;

                Scheduler.RunSynchronously(async () =>
                {
                    Assert.IsNull(SynchronizationContext.Current);
                    Assert.IsInstanceOfType(TaskScheduler.Current, typeof(ActiveThreadScheduler));
                    Assert.AreEqual(threadId, ThreadId());

                    await DummyWork().AutoConfigureAwait();

                    Assert.IsNull(SynchronizationContext.Current);
                    Assert.IsInstanceOfType(TaskScheduler.Current, typeof(ActiveThreadScheduler));
                    Assert.AreEqual(threadId, ThreadId());
                });

                Assert.AreEqual(context, SynchronizationContext.Current);
                Assert.AreEqual(scheduler, TaskScheduler.Current);
                Assert.AreEqual(threadId, ThreadId());
            });
        }

        [TestMethod]
        public void DelayAsync()
        {
            SyncContextHelper.Invoke(async () =>
            {
                var threadId = ThreadId();
                Assert.IsNotNull(SynchronizationContext.Current);
                
                await Task.Delay(10).AutoConfigureAwait();

                Assert.AreNotEqual(threadId, ThreadId());
                Assert.IsNull(SynchronizationContext.Current);
            });
        }

        [TestMethod]
        public void DelayWithResult()
        {
            SyncContextHelper.Invoke(() =>
            {

                var threadId = ThreadId();
                var scheduler = TaskScheduler.Current;
                var context = SynchronizationContext.Current;

                const int value = 100;
                int actual = Scheduler.RunSynchronously(async () =>
                {
                    Assert.IsNull(SynchronizationContext.Current);
                    Assert.IsInstanceOfType(TaskScheduler.Current, typeof(ActiveThreadScheduler));
                    Assert.AreEqual(threadId, ThreadId());

                    await Task.Delay(10).AutoConfigureAwait();

                    Assert.IsNull(SynchronizationContext.Current);
                    Assert.IsInstanceOfType(TaskScheduler.Current, typeof(ActiveThreadScheduler));
                    Assert.AreEqual(threadId, ThreadId());

                    return value;
                });

                Assert.AreEqual(value, actual);
                Assert.AreEqual(context, SynchronizationContext.Current);
                Assert.AreEqual(scheduler, TaskScheduler.Current);
                Assert.AreEqual(threadId, ThreadId());
            });
        }

        [TestMethod]
        public void DelayThenException()
        {
            SyncContextHelper.Invoke(() =>
            {

                var threadId = ThreadId();
                try
                {
                    Scheduler.RunSynchronously(async () =>
                    {
                        Assert.AreEqual(threadId, ThreadId());
                        await Task.Delay(10).AutoConfigureAwait();
                        Assert.AreEqual(threadId, ThreadId());
                        throw new Exception("Yikes");
                    });

                }
                catch(Exception ex)
                {
                    Assert.AreEqual("Yikes", ex.Message);
                    return;
                }

                Assert.Fail("Exception expected");
            });
        }

        public async Task DummyWork()
        {
            await Task.Delay(10).AutoConfigureAwait();
        }
    }
}