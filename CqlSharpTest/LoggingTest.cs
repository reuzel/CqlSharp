using CqlSharp.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Diagnostics;

namespace CqlSharp.Test
{
    [TestClass]
    class LoggingTest
    {

        [TestMethod]
        public void FastGuidIsFast()
        {
            const int count = 100000;

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            for (int i = 0; i < count; i++)
            {
                Guid g = Guid.NewGuid();
            }

            stopwatch.Stop();
            TimeSpan slow = stopwatch.Elapsed;

            stopwatch.Reset();
            stopwatch.Start();

            for (int i = 0; i < count; i++)
            {
                Guid g = FastGuid.NewGuid();
            }

            stopwatch.Stop();
            TimeSpan fast = stopwatch.Elapsed;

            Assert.IsTrue(slow > fast);
        }
    }
}
