using System;
using System.Threading.Tasks;

namespace CqlSharp.Threading
{
    /// <summary>
    /// Awaitable that automatically determines how continuations need to be registered
    /// </summary>
    internal struct AutoConfiguredAwaitable
    {
        private readonly AutoConfiguredAwaiter _awaiter;

        /// <summary>
        /// Initializes a new instance of the <see cref="AutoConfiguredAwaitable"/> struct.
        /// </summary>
        /// <param name="task">The task being awaited</param>
        /// <exception cref="System.ArgumentNullException">task</exception>
        public AutoConfiguredAwaitable(Task task)
        {
            if(task == null) throw new ArgumentNullException("task");
            _awaiter = new AutoConfiguredAwaiter(task);
        }

        public AutoConfiguredAwaiter GetAwaiter()
        {
            return _awaiter;
        }

        /// <summary>
        /// Schedules the continuation action that's invoked when the instance completes.
        /// </summary>
        /// <param name="task">task to which continuation is to be registered</param>
        /// <param name="continuation">The action to invoke when the operation completes.</param><exception cref="T:System.ArgumentNullException">The <paramref name="continuation"/> argument is null (Nothing in Visual Basic).</exception>
        internal static void OnCompleted(Task task, Action continuation)
        {
            var captureContext = NeedToConfigure();
            var awaiter = task.ConfigureAwait(captureContext).GetAwaiter();
            awaiter.OnCompleted(continuation);
        }

        /// <summary>
        /// Schedules the continuation action that's invoked when the instance completes.
        /// </summary>
        /// <param name="task">task to which continuation is to be registered</param>
        /// <param name="continuation">The action to invoke when the operation completes.</param><exception cref="T:System.ArgumentNullException">The <paramref name="continuation"/> argument is null (Nothing in Visual Basic).</exception>
        internal static void UnsafeOnCompleted(Task task, Action continuation)
        {
            var captureContext = NeedToConfigure();
            var awaiter = task.ConfigureAwait(captureContext).GetAwaiter();
            awaiter.UnsafeOnCompleted(continuation);
        }

        /// <summary>
        /// Checks if there is a need to continue on the current context
        /// </summary>
        /// <returns></returns>
        private static bool NeedToConfigure()
        {
            var scheduler = TaskScheduler.Current;
            return (scheduler is IOTaskScheduler || scheduler is ActiveThreadScheduler);
        }
    }

    /// <summary>
    /// Awaitable that automatically determines how continuations need to be registered
    /// </summary>
    internal struct AutoConfiguredAwaitable<T>
    {
        private readonly AutoConfiguredAwaiter<T> _awaiter;

        /// <summary>
        /// Initializes a new instance of the <see cref="AutoConfiguredAwaitable"/> struct.
        /// </summary>
        /// <param name="task">The task being awaited</param>
        /// <exception cref="System.ArgumentNullException">task</exception>
        public AutoConfiguredAwaitable(Task<T> task)
        {
            if(task == null) throw new ArgumentNullException("task");
            _awaiter = new AutoConfiguredAwaiter<T>(task);
        }

        public AutoConfiguredAwaiter<T> GetAwaiter()
        {
            return _awaiter;
        }
    }

}