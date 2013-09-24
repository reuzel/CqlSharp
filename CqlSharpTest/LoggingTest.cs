// CqlSharp - CqlSharp.Test
// Copyright (c) 2013 Joost Reuzel
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

using CqlSharp.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Diagnostics;

namespace CqlSharp.Test
{
    [TestClass]
    public class LoggingTest
    {
        //[TestMethod]
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