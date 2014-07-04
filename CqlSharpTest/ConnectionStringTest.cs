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
using System.Globalization;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Test
{
    [TestClass]
    public class ConnectionStringTest
    {
        private static readonly TextInfo TextInfo = new CultureInfo("en-US", false).TextInfo;

        [TestMethod]
        public void CommandTimeout()
        {
            CheckCommand("command timeout", 10, c => c.CommandTimeout);
        }

        [TestMethod]
        public void SocketReceiveBufferSize()
        {
            CheckCommand("socket receive buffer size", 11, c => c.SocketReceiveBufferSize);
        }

        private void CheckCommand<T>(string command, T value, Func<CqlConnectionStringBuilder, T> getter)
        {
            Assert.AreEqual(value, getter(new CqlConnectionStringBuilder(command + "=" + value)));
            // checking with spaces removed
            Assert.AreEqual(value, getter(new CqlConnectionStringBuilder(command.Replace(" ", "") + "=" + value)));
            // checking with capitalized letters
            Assert.AreEqual(value, getter(new CqlConnectionStringBuilder(TextInfo.ToTitleCase(command) + "=" + value)));
        }
    }
}