using System;
using System.Diagnostics;
using System.Threading;
using System.Windows.Threading;

namespace CqlSharpTest
{

    /// <summary>
    /// Helper context to mimic constrained synchronization contexts.
    /// Obtained from: http://stackoverflow.com/questions/1882417/looking-for-an-example-of-a-custom-synchronizationcontext
    /// </summary>
    public class STASynchronizationContext : SynchronizationContext, IDisposable
    {
        private readonly Dispatcher _dispatcher;
        private object _dispObj;
        private readonly Thread _mainThread;

        public STASynchronizationContext()
        {
            _mainThread = new Thread(MainThread) { Name = "STASynchronizationContextMainThread", IsBackground = false };
            _mainThread.SetApartmentState(ApartmentState.STA);
            _mainThread.Start();

            //wait to get the main thread's dispatcher
            while (Thread.VolatileRead(ref _dispObj) == null)
                Thread.Yield();

            _dispatcher = _dispObj as Dispatcher;
        }

        public override void Post(SendOrPostCallback d, object state)
        {
            _dispatcher.BeginInvoke(d, new object[] { state });
        }

        public override void Send(SendOrPostCallback d, object state)
        {
            _dispatcher.Invoke(d, new object[] { state });
        }

        private void MainThread(object param)
        {
            Thread.VolatileWrite(ref _dispObj, Dispatcher.CurrentDispatcher);
            Debug.WriteLine("Main Thread is setup ! Id = {0}", Thread.CurrentThread.ManagedThreadId);
            Dispatcher.Run();
        }

        public void Dispose()
        {
            if (!_dispatcher.HasShutdownStarted && !_dispatcher.HasShutdownFinished)
                _dispatcher.BeginInvokeShutdown(DispatcherPriority.Normal);

            GC.SuppressFinalize(this);
        }

        ~STASynchronizationContext()
        {
            Dispose();
        }
    }
}
