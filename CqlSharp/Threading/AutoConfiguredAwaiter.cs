using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

namespace CqlSharp.Threading
{
    /// <summary>
    /// Awaiter that automatically determines how awaits need to be continued
    /// </summary>
    internal struct AutoConfiguredAwaiter : ICriticalNotifyCompletion
    {
        private readonly Task _task;

        internal AutoConfiguredAwaiter(Task task)
        {
            _task = task;
        }

        /// <summary>
        /// Gets a value indicating whether this instance is completed. Always
        /// returns true, as a synchronous call is always completed as soon
        /// as we return from it.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is completed; otherwise, <c>false</c>.
        /// </value>
        public bool IsCompleted
        {
            get { return _task.IsCompleted; }
        }

        /// <summary>
        /// Gets the result.
        /// </summary>
        /// <returns></returns>
        public void GetResult()
        {
            try
            {
                Debug.WriteLine("Calling Task.Wait()");
                _task.Wait();
            }
            catch(AggregateException aex)
            {
                ExceptionDispatchInfo.Capture(aex.InnerException).Throw();
            }
        }

        /// <summary>
        /// Schedules the continuation action that's invoked when the instance completes.
        /// </summary>
        /// <param name="continuation">The action to invoke when the operation completes.</param><exception cref="T:System.ArgumentNullException">The <paramref name="continuation"/> argument is null (Nothing in Visual Basic).</exception>
        public void OnCompleted(Action continuation)
        {
            AutoConfiguredAwaitable.OnCompleted(_task, continuation);
        }

        /// <summary>
        /// Schedules the continuation action that's invoked when the instance completes.
        /// </summary>
        /// <param name="continuation">The action to invoke when the operation completes.</param><exception cref="T:System.ArgumentNullException">The <paramref name="continuation"/> argument is null (Nothing in Visual Basic).</exception>
        public void UnsafeOnCompleted(Action continuation)
        {
            AutoConfiguredAwaitable.UnsafeOnCompleted(_task, continuation);
        }
    }

    /// <summary>
    /// Awaiter that automatically determines how awaits need to be continued
    /// </summary>
    internal struct AutoConfiguredAwaiter<T> : ICriticalNotifyCompletion
    {
        private readonly Task<T> _task;

        internal AutoConfiguredAwaiter(Task<T> task)
        {
            _task = task;
        }

        /// <summary>
        /// Gets a value indicating whether this instance is completed. Always
        /// returns true, as a synchronous call is always completed as soon
        /// as we return from it.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is completed; otherwise, <c>false</c>.
        /// </value>
        public bool IsCompleted
        {
            get { return _task.IsCompleted; }
        }

        /// <summary>
        /// Gets the result.
        /// </summary>
        /// <returns></returns>
        public T GetResult()
        {
            try
            {
                Debug.WriteLine("Calling Task.Result");
                return _task.Result;
            }
            catch (AggregateException aex)
            {
                ExceptionDispatchInfo.Capture(aex.InnerException).Throw();
                throw; //dead code, but required to compile
            }
        }

        /// <summary>
        /// Schedules the continuation action that's invoked when the instance completes.
        /// </summary>
        /// <param name="continuation">The action to invoke when the operation completes.</param><exception cref="T:System.ArgumentNullException">The <paramref name="continuation"/> argument is null (Nothing in Visual Basic).</exception>
        public void OnCompleted(Action continuation)
        {
            AutoConfiguredAwaitable.OnCompleted(_task, continuation);
        }

        /// <summary>
        /// Schedules the continuation action that's invoked when the instance completes.
        /// </summary>
        /// <param name="continuation">The action to invoke when the operation completes.</param><exception cref="T:System.ArgumentNullException">The <paramref name="continuation"/> argument is null (Nothing in Visual Basic).</exception>
        public void UnsafeOnCompleted(Action continuation)
        {
            AutoConfiguredAwaitable.UnsafeOnCompleted(_task, continuation);
        }
    }
}