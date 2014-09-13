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
using System.Security;
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
        /// Initializes a new instance of the <see cref="AutoConfiguredAwaitable" /> struct.
        /// </summary>
        /// <param name="task">The task being awaited</param>
        /// <exception cref="System.ArgumentNullException">task</exception>
        public AutoConfiguredAwaitable(Task task)
        {
            if(task == null) throw new ArgumentNullException("task");
            _awaiter = new AutoConfiguredAwaiter(task);
        }

        // ReSharper disable once UnusedMethodReturnValue.Global
        public AutoConfiguredAwaiter GetAwaiter()
        {
            return _awaiter;
        }

        /// <summary>
        /// Schedules the continuation action that's invoked when the instance completes.
        /// </summary>
        /// <param name="task">task to which continuation is to be registered</param>
        /// <param name="continuation">The action to invoke when the operation completes.</param>
        /// <exception cref="T:System.ArgumentNullException">
        /// The <paramref name="continuation" /> argument is null (Nothing in
        /// Visual Basic).
        /// </exception>
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
        /// <param name="continuation">The action to invoke when the operation completes.</param>
        /// <exception cref="T:System.ArgumentNullException">
        /// The <paramref name="continuation" /> argument is null (Nothing in
        /// Visual Basic).
        /// </exception>
        [SecurityCritical]
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
        /// Initializes a new instance of the <see cref="AutoConfiguredAwaitable" /> struct.
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