// CqlSharp - CqlSharp.Test
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
using System.Diagnostics;
using System.Dynamic;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Threading;

namespace CqlSharp.Test
{
    /// <summary>
    /// Helper context to mimic constrained synchronization contexts.
    /// Obtained from: http://stackoverflow.com/questions/1882417/looking-for-an-example-of-a-custom-synchronizationcontext
    /// </summary>
    public class SyncContextHelper : IDisposable
    {
        private readonly Dispatcher _dispatcher;
        private object _dispObj;

        private SyncContextHelper()
        {
            var mainThread = new Thread(MainThread) {Name = "STASynchronizationContextMainThread", IsBackground = false};
            mainThread.SetApartmentState(ApartmentState.STA);
            mainThread.Start();

            //wait to get the main thread's dispatcher
            while(Thread.VolatileRead(ref _dispObj) == null)
                Thread.Yield();

            _dispatcher = _dispObj as Dispatcher;
        }

        #region IDisposable Members

        public void Dispose()
        {
            if(!_dispatcher.HasShutdownStarted && !_dispatcher.HasShutdownFinished)
                _dispatcher.BeginInvokeShutdown(DispatcherPriority.Normal);

            GC.SuppressFinalize(this);
        }

        #endregion

        public static void Invoke(Action action)
        {
            var helper = new SyncContextHelper();
            helper._dispatcher.Invoke(action);
            helper.Dispose();
        }

        public static void Invoke(Func<Task> action)
        {
            var helper = new SyncContextHelper();
            var task = helper._dispatcher.Invoke(action);
            task.Wait();
            helper.Dispose();
        }

        private void MainThread(object param)
        {
            Thread.VolatileWrite(ref _dispObj, Dispatcher.CurrentDispatcher);
            Debug.WriteLine("Main Thread is setup ! Id = {0}", Thread.CurrentThread.ManagedThreadId);
            Dispatcher.Run();
        }

        ~SyncContextHelper()
        {
            Dispose();
        }
    }
}