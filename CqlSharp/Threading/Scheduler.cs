using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Schedulers;

namespace CqlSharp.Threading
{
    /// <summary>
    /// Controls on which threads the different tasks are scheduled
    /// </summary>
    internal static class Scheduler
    {
        private static readonly TaskScheduler IOScheduler = new IOTaskScheduler();
        private static readonly Task CompletedTask = Task.FromResult(false);

        public static void RunOnIOThread(Action task)
        {
            if(TaskScheduler.Current != IOScheduler)
                Task.Factory.StartNew(task, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IOScheduler);
            else
                task();
        }

        public static Task RunOnIOThread(Func<Task> task)
        {
            if(TaskScheduler.Current != IOScheduler)
            {
                return
                    Task.Factory.StartNew(task, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IOScheduler)
                        .Unwrap();
            }

            return task();
        }

        public static Task<T> RunOnIOThread<T>(Func<Task<T>> task)
        {
            if(TaskScheduler.Current != IOScheduler)
            {
                return Task.Factory.StartNew(task, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IOScheduler)
                    .Unwrap();
            }

            return task();
        }

        /// <summary>
        /// Runs a task as much as possible on the current thread. This can be regarded as an aggressive form of inlining.
        /// </summary>
        /// <param name="task">The task.</param>
        public static void RunSynchronously(Func<Task> task)
        {
            //create scheduler to run the task on the current thread
            var scheduler = new ActiveThreadScheduler();

            //schedule the task (on the current thread)
            var executedTask = Task.Factory.StartNew(task, CancellationToken.None, TaskCreationOptions.DenyChildAttach, scheduler)
                                   .Unwrap()
                                   .ContinueWith(previous =>
                                   {
                                       scheduler.Complete();
                                       previous.Wait();
                                   }, TaskContinuationOptions.ExecuteSynchronously);

            //start task execution loop to run all continuations and sub-tasks on the current thread, until main task completes
            scheduler.ExecuteTasks();

            //return result
            try
            {
                executedTask.Wait();
            }
            catch (AggregateException aex)
            {
                //return any inner exception. Note that this will kill the call-stack...
                throw aex.Flatten().InnerException;
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
            var scheduler = new ActiveThreadScheduler();
            var executedTask = Task.Factory.StartNew(task, CancellationToken.None, TaskCreationOptions.DenyChildAttach, scheduler)
                                   .Unwrap()
                                   .ContinueWith(previous =>
                                   {
                                       scheduler.Complete();
                                       return previous.Result;
                                   }, TaskContinuationOptions.ExecuteSynchronously);

            scheduler.ExecuteTasks();

            try
            {
                return executedTask.Result;
            }
            catch (AggregateException aex)
            {
                throw aex.Flatten().InnerException;
            }
        }

        /// <summary>
        /// Runs the action on the reguler .NET thread pool. Used to escape the IO pool.
        /// </summary>
        /// <param name="action">The action.</param>
        /// <param name="forceNewTask">force the creation of a new task</param>
        /// <returns></returns>
        public static Task RunOnThreadPool(Action action, bool forceNewTask = false)
        {
            if (forceNewTask || (TaskScheduler.Current != TaskScheduler.Default))
                return Task.Run(action);

            //already on thread pool, just execute action inline
            action();
            return CompletedTask;
        }
    }
}
