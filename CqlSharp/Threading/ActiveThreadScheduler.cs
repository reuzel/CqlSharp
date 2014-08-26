using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp.Threading
{
    /// <summary>
    /// Runs all tasks on the thread used to create the scheduler
    /// </summary>
    internal sealed class ActiveThreadScheduler : TaskScheduler
    {
        private readonly Thread _thread;
        private readonly LinkedList<Task> _tasks;
        private bool _completed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ActiveThreadScheduler"/> class.
        /// </summary>
        public ActiveThreadScheduler()
            : this(Thread.CurrentThread)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ActiveThreadScheduler"/> class.
        /// </summary>
        /// <param name="thread">The thread.</param>
        /// <exception cref="System.ArgumentNullException">thread</exception>
        public ActiveThreadScheduler(Thread thread)
        {
            if (thread == null) throw new ArgumentNullException("thread");

            _tasks = new LinkedList<Task>();
            _thread = thread;
            _completed = false;
        }

        /// <summary>
        /// Queues a <see cref="T:System.Threading.Tasks.Task"/> to the scheduler.
        /// </summary>
        /// <param name="task">The <see cref="T:System.Threading.Tasks.Task"/> to be queued.</param><exception cref="T:System.ArgumentNullException">The <paramref name="task"/> argument is null.</exception>
        protected override void QueueTask(Task task)
        {
            bool inline = false;

            lock (_tasks)
            {
                if(_completed)
                {
                    //thread no longer available, so inline this task with the enqueuing thread.
                    //May occur when queries are cancelled, which may lead to some parallelism.
                    inline = true;
                }
                else
                {
                    _tasks.AddLast(task);
                    Monitor.PulseAll(_tasks);
                }
            }

            if(inline)
                TryExecuteTask(task);
        }

        /// <summary>
        /// Determines whether the provided <see cref="T:System.Threading.Tasks.Task"/> can be executed synchronously in this call, and if it can, executes it.
        /// </summary>
        /// <returns>
        /// A Boolean value indicating whether the task was executed inline.
        /// </returns>
        /// <param name="task">The <see cref="T:System.Threading.Tasks.Task"/> to be executed.</param><param name="taskWasPreviouslyQueued">A Boolean denoting whether or not task has previously been queued. If this parameter is True, then the task may have been previously queued (scheduled); if False, then the task is known not to have been queued, and this call is being made in order to execute the task inline without queuing it.</param><exception cref="T:System.ArgumentNullException">The <paramref name="task"/> argument is null.</exception><exception cref="T:System.InvalidOperationException">The <paramref name="task"/> was already executed.</exception>
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            if (Thread.CurrentThread != _thread)
                return false;

            if (taskWasPreviouslyQueued)
            {
                if (_tasks.Remove(task))
                {
                    return TryExecuteTask(task);
                }

                return false;
            }

            return TryExecuteTask(task);
        }

        /// <summary>
        /// For debugger support only, generates an enumerable of <see cref="T:System.Threading.Tasks.Task"/> instances currently queued to the scheduler waiting to be executed.
        /// </summary>
        /// <returns>
        /// An enumerable that allows a debugger to traverse the tasks currently queued to this scheduler.
        /// </returns>
        /// <exception cref="T:System.NotSupportedException">This scheduler is unable to generate a list of queued tasks at this time.</exception>
        protected override IEnumerable<Task> GetScheduledTasks()
        {
            lock (_tasks)
                return _tasks.ToArray();
        }

        /// <summary>
        /// Start execution of tasks Scheduled to this scheduler.
        /// </summary>
        /// <exception cref="System.InvalidOperationException">Execute must be called using the same thread as used to construct this scheduler</exception>
        public void ExecuteTasks()
        {
            if (Thread.CurrentThread != _thread)
                throw new InvalidOperationException("Execute must be called using the same thread as used to construct this scheduler");

            Task task;
            while (TryTake(out task))
            {
                if (!TryExecuteTask(task))
                    break;
            }
        }

        /// <summary>
        /// Tries to take the first task from the set. This operation will block until an task has become available, or no more
        /// additions are expected.
        /// </summary>
        /// <param name="task">The task taken</param>
        /// <returns>
        /// true, if an task was taken from the set. False, if no items can be retrieved. False will only be returned when
        /// the set is empty, and adding has been completed.
        /// </returns>
        private bool TryTake(out Task task)
        {
            lock (_tasks)
            {
                var node = _tasks.First;

                //go sit in a loop waiting for an task to arrive, or adding completed
                while (node == null)
                {
                    if (_completed)
                    {
                        task = null;
                        return false;
                    }

                    Monitor.Wait(_tasks);
                    node = _tasks.First;
                }

                _tasks.Remove(node);
                task = node.Value;
                return true;
            }
        }

        /// <summary>
        /// Signal to this scheduler that no more tasks are expected to arrive on this scheduler
        /// </summary>
        public void Complete()
        {
            lock (_tasks)
            {
                _completed = true;
                Monitor.PulseAll(_tasks);
            }
        }
    }
}
