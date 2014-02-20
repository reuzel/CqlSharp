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
using System.Collections.Concurrent;
using System.Numerics;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Test
{
    [TestClass]
    public class TimeUuidTest
    {
        [TestMethod]
        public void TimeUuidIssue()
        {
            // this test uses BigInteger to check, otherwise the Dictionary
            // will complain because Guid's GetHashCode will collide
            var timestamps = new ConcurrentDictionary<BigInteger, Guid>();

            Action runner = delegate
                                {
                                    // run a full clock sequence cycle (or so)
                                    for (var n = 0; n < 10001; n++)
                                    {
                                        var time = DateTime.UtcNow;
                                        var guid = time.GenerateTimeBasedGuid();
                                        var bigint = new BigInteger(guid.ToByteArray());

                                        Assert.IsTrue(timestamps.TryAdd(bigint, guid), "Key already exists!");
                                        Assert.AreEqual(time.ToTimestamp(), guid.GetDateTime().ToTimestamp());
                                    }
                                };

            Parallel.Invoke(runner, runner, runner, runner);
        }

        [TestMethod]
        public void TimeUuidRoundTrip()
        {
            var time = DateTime.UtcNow;
            var guid = time.GenerateTimeBasedGuid();
            var time2 = guid.GetDateTime();

            Assert.AreEqual(time, time2);
        }

        [TestMethod]
        public void ValidateTimeUuidGetDateTime()
        {
            const string timeUuid = "92ea3200-9a80-11e3-9669-0800200c9a66";
            var expected = new DateTime(2014, 2, 20, 22, 44, 36, 256, DateTimeKind.Utc);

            var guid = Guid.Parse(timeUuid);
            var actual = guid.GetDateTime();

            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void ValidateTimeUuidGeneration()
        {
            const string expected = "92ea3200-9a80-11e3-9669-0800200c9a66";

            var dateTime = new DateTime(2014, 2, 20, 22, 44, 36, 256, DateTimeKind.Utc);
            var time = (dateTime - TimeGuid.GregorianCalendarStart).Ticks;
            var node = new byte[] {0x08, 0x00, 0x20, 0x0c, 0x9a, 0x66};
            const int clockId = 5737;

            var guid = TimeGuid.GenerateTimeBasedGuid(time, clockId, node);
            var actual = guid.ToString("D").ToLower();

            Assert.AreEqual(expected, actual);
        }
    }
}