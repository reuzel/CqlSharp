//--------------------------------------------------------------------------
// 
//  Copyright (c) Microsoft Corporation.  All rights reserved. 
// 
//  File: IOTaskScheduler.cs
//
//--------------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp.Threading
{
    /// <summary>Provides a task scheduler that targets the I/O ThreadPool.</summary>
    /// <remarks>Based on the IOTaskScheduler as provided here: <see cref="http://code.msdn.microsoft.com/Samples-for-Parallel-b4b76364/sourcecode?fileId=44488&pathId=1257700934"/></remarks>
    internal sealed class IOTaskScheduler : TaskScheduler, IDisposable
    {
        /// <summary>Represents a task queued to the I/O pool.</summary>
        private unsafe class WorkItem : IDisposable
        {
            private readonly IOTaskScheduler _scheduler;
            private readonly NativeOverlapped* _pNOlap;
            internal Task Task;

            public WorkItem(IOTaskScheduler scheduler)
            {
                _scheduler = scheduler;
                _pNOlap = new Overlapped().UnsafePack(Callback, null);
            }

            private void Callback(uint errorCode, uint numBytes, NativeOverlapped* pNOlap)
            {
                // Execute the task
                _scheduler.TryExecuteTask(Task);

                // Put this item back into the pool for someone else to use
                var pool = _scheduler._availableWorkItems;
                if (pool != null) pool.Add(this);
                else Overlapped.Free(pNOlap);
            }

            /// <summary>
            /// Enqueues this instance to the IO thread pool.
            /// </summary>
            internal void Enqueue()
            {
                ThreadPool.UnsafeQueueNativeOverlapped(_pNOlap);
            }

            /// <summary>
            /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
            /// </summary>
            public void Dispose()
            {
                Overlapped.Free(_pNOlap);
            }
        }

        // A pool of available WorkItem instances that can be used to schedule tasks
        private ConcurrentBag<WorkItem> _availableWorkItems;

        /// <summary>
        /// Gets or create a work item.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="System.ObjectDisposedException"></exception>
        private WorkItem GetOrCreateWorkItem()
        {
            var pool = _availableWorkItems;
            if (pool == null) throw new ObjectDisposedException(GetType().Name);

            WorkItem item;
            if(!pool.TryTake(out item))
            {
                item = new WorkItem(this);
            }

            return item;
        }

        /// <summary>Initializes a new instance of the IOTaskScheduler class.</summary>
        public IOTaskScheduler()
        {
            // Configure the object pool of work items
            _availableWorkItems = new ConcurrentBag<WorkItem>();
        }

        /// <summary>Queues a task to the scheduler for execution on the I/O ThreadPool.</summary>
        /// <param name="task">The Task to queue.</param>
        protected override void QueueTask(Task task)
        {
            var wi = GetOrCreateWorkItem();
            wi.Task = task;
            wi.Enqueue();
        }

        /// <summary>Executes a task on the current thread.</summary>
        /// <param name="task">The task to be executed.</param>
        /// <param name="taskWasPreviouslyQueued">Ignored.</param>
        /// <returns>Whether the task could be executed.</returns>
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            return TryExecuteTask(task);
        }

        /// <summary>Disposes of resources used by the scheduler.</summary>
        public void Dispose()
        {
            var pool = _availableWorkItems;
            _availableWorkItems = null;

            //take and dispose all items in the current pool
            WorkItem item;
            while(pool.TryTake(out item))
                item.Dispose();
            
            // NOTE: A window exists where some number of NativeOverlapped ptrs could
            // be leaked, if the call to Dispose races with work items completing.
        }

        /// <summary>Gets an enumerable of tasks queued to the scheduler.</summary>
        /// <returns>An enumerable of tasks queued to the scheduler.</returns>
        /// <remarks>This implementation will always return an empty enumerable.</remarks>
        protected override IEnumerable<Task> GetScheduledTasks() { return Enumerable.Empty<Task>(); }
    }
}