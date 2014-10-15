// CqlSharp - CqlSharp
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
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp.Threading
{
    /// <summary>
    /// Controls on which threads the different tasks are scheduled
    /// </summary>
    internal static class Scheduler
    {
        private static readonly TaskScheduler IOScheduler = new IOTaskScheduler();

        public static void RunOnIOThread(Action task)
        {
            Task.Factory.StartNew(task, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IOScheduler);
        }

        public static Task RunOnIOThread(Func<Task> task)
        {
            return Task.Factory.StartNew(task, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IOScheduler)
                       .Unwrap();
        }

        public static Task<T> RunOnIOThread<T>(Func<Task<T>> task)
        {
            return Task.Factory.StartNew(task, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IOScheduler)
                       .Unwrap();
        }

        /// <summary>
        /// Runs a task as much as possible on the current thread. This can be regarded as an aggressive form of inlining.
        /// </summary>
        /// <param name="task">The task.</param>
        public static void RunSynchronously(Func<Task> task)
        {
            //capture and clear context
            var currentContext = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(null);

            //create scheduler to run the task on the current thread
            var scheduler = new ActiveThreadScheduler();

            //schedule the task (on the current thread)
            var executedTask = Task.Factory.StartNew(task, CancellationToken.None, TaskCreationOptions.DenyChildAttach,
                                                     scheduler)
                                   .Unwrap()
                                   .ContinueWith(previous =>
                                   {
                                       scheduler.Complete();
                                       previous.Wait();
                                   }, TaskContinuationOptions.ExecuteSynchronously);

            //start task execution loop to run all continuations and sub-tasks on the current thread, until main task completes
            scheduler.ExecuteTasks();

            //restore context
            SynchronizationContext.SetSynchronizationContext(currentContext);

            //return result
            try
            {
                executedTask.Wait();
            }
            catch(AggregateException aex)
            {
                //return any inner exception.
                ExceptionDispatchInfo.Capture(aex.Flatten().InnerException).Throw();
            }
        }

        /// <summary>
        /// Runs a task as much as possible on the current thread. This can be regarded as an aggressive form of inlining.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="task">The task.</param>
        /// <returns></returns>
        public static T RunSynchronously<T>(Func<Task<T>> task)
        {
            //capture and clear context
            var currentContext = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(null);

            //create scheduler to run the task on the current thread
            var scheduler = new ActiveThreadScheduler();

            //schedule the task (on the current thread)
            var executedTask = Task.Factory.StartNew(task, CancellationToken.None, TaskCreationOptions.DenyChildAttach,
                                                     scheduler)
                                   .Unwrap()
                                   .ContinueWith(previous =>
                                   {
                                       scheduler.Complete();
                                       return previous.Result;
                                   }, TaskContinuationOptions.ExecuteSynchronously);

            //start task execution loop to run all continuations and sub-tasks on the current thread, until main task completes
            scheduler.ExecuteTasks();

            //restore context
            SynchronizationContext.SetSynchronizationContext(currentContext);

            //return result
            try
            {
                return executedTask.Result;
            }
            catch(AggregateException aex)
            {
                //return any inner exception. 
                ExceptionDispatchInfo.Capture(aex.Flatten().InnerException).Throw();
                throw;
            }
        }

        /// <summary>
        /// Runs the action on the reguler .NET thread pool. Used to escape the IO pool.
        /// </summary>
        /// <param name="action">The action.</param>
        public static void RunOnThreadPool(Action action)
        {
            Task.Run(action);
        }

        /// <summary>
        /// Gets a value indicating whether [running synchronously].
        /// </summary>
        /// <value>
        /// <c>true</c> if [running synchronously]; otherwise, <c>false</c>.
        /// </value>
        public static bool RunningSynchronously
        {
            get { return TaskScheduler.Current is ActiveThreadScheduler; }
        }

        /// <summary>
        /// Automatically the configures the await depending on the type of scheduler
        /// </summary>
        /// <param name="task">The task "to be awaited".</param>
        /// <returns></returns>
        public static AutoConfiguredAwaitable AutoConfigureAwait(this Task task)
        {
            return new AutoConfiguredAwaitable(task);
        }

        /// <summary>
        /// Automatically the configures the await depending on the type of scheduler
        /// </summary>
        /// <param name="task">The task "to be awaited".</param>
        /// <returns></returns>
        public static AutoConfiguredAwaitable<T> AutoConfigureAwait<T>(this Task<T> task)
        {
            return new AutoConfiguredAwaitable<T>(task);
        }
    }
}