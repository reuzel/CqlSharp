using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Test
{
    [TestClass]
    public class ConnectionStringTest
    {
        private static System.Globalization.TextInfo TextInfo = new System.Globalization.CultureInfo("en-US",false).TextInfo;

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