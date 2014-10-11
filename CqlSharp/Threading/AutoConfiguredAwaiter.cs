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
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Security;
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
        /// <param name="continuation">The action to invoke when the operation completes.</param>
        /// <exception cref="T:System.ArgumentNullException">
        /// The <paramref name="continuation" /> argument is null (Nothing in
        /// Visual Basic).
        /// </exception>
        public void OnCompleted(Action continuation)
        {
            AutoConfiguredAwaitable.OnCompleted(_task, continuation);
        }

        /// <summary>
        /// Schedules the continuation action that's invoked when the instance completes.
        /// </summary>
        /// <param name="continuation">The action to invoke when the operation completes.</param>
        /// <exception cref="T:System.ArgumentNullException">
        /// The <paramref name="continuation" /> argument is null (Nothing in
        /// Visual Basic).
        /// </exception>
        [SecurityCritical]
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
                return _task.Result;
            }
            catch(AggregateException aex)
            {
                ExceptionDispatchInfo.Capture(aex.InnerException).Throw();
                throw;
            }
        }

        /// <summary>
        /// Schedules the continuation action that's invoked when the instance completes.
        /// </summary>
        /// <param name="continuation">The action to invoke when the operation completes.</param>
        /// <exception cref="T:System.ArgumentNullException">
        /// The <paramref name="continuation" /> argument is null (Nothing in
        /// Visual Basic).
        /// </exception>
        public void OnCompleted(Action continuation)
        {
            AutoConfiguredAwaitable.OnCompleted(_task, continuation);
        }

        /// <summary>
        /// Schedules the continuation action that's invoked when the instance completes.
        /// </summary>
        /// <param name="continuation">The action to invoke when the operation completes.</param>
        /// <exception cref="T:System.ArgumentNullException">
        /// The <paramref name="continuation" /> argument is null (Nothing in
        /// Visual Basic).
        /// </exception>
        [SecurityCritical]
        public void UnsafeOnCompleted(Action continuation)
        {
            AutoConfiguredAwaitable.UnsafeOnCompleted(_task, continuation);
        }
    }
}