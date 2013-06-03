// CqlSharp - CqlSharp
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

using System;
using System.Diagnostics;
using System.Threading;

namespace CqlSharp.Logging
{
    /// <summary>
    /// Represents a logger for a single logical flow through the library
    /// </summary>
    internal class Logger
    {
        /// <summary>
        ///   Thread local storage of the currently active logger.
        /// </summary>
        private static readonly ThreadLocal<Logger> ThreadLocalLogger = new ThreadLocal<Logger>();

        /// <summary>
        ///   helper class instance representing a binding of a logger to a thread. Does not contain any state
        ///   and can therefore be reused among all logger instances.
        /// </summary>
        private static readonly LoggerBinding Binding = new LoggerBinding();

        /// <summary>
        ///   Initializes a new instance of the <see cref="Logger" /> class.
        /// </summary>
        /// <param name="name"> The name. </param>
        public Logger(string name)
        {
            Name = name;
        }

        /// <summary>
        ///   Gets the current Logger.
        /// </summary>
        /// <value> The current logger </value>
        public static Logger Current
        {
            get { return ThreadLocalLogger.Value; }
        }

        /// <summary>
        ///   Gets or sets the name.
        /// </summary>
        /// <value> The name. </value>
        public string Name { get; set; }

        /// <summary>
        ///   Binds this instance to the current thread, such that it becomes available via Logger.Current
        /// </summary>
        /// <returns> IDisposable, that when disposed removes the logger from the current thread </returns>
        public IDisposable ThreadBinding()
        {
            ThreadLocalLogger.Value = this;
            return Binding;
        }

        public void LogVerbose(string format, params object[] values)
        {
            Debug.WriteLine(format, values);
        }

        public void LogInfo(string format, params object[] values)
        {
            Debug.WriteLine(format, values);
        }

        public void LogWarning(string format, params object[] values)
        {
            Debug.WriteLine(format, values);
        }

        public void LogError(string format, params object[] values)
        {
            Debug.WriteLine(format, values);
        }

        public void LogCritical(string format, params object[] values)
        {
            Debug.WriteLine(format, values);
        }

        #region Nested type: LoggerBinding

        /// <summary>
        ///   Helper class to remove a logger from the thread local storage
        /// </summary>
        private class LoggerBinding : IDisposable
        {
            #region Implementation of IDisposable

            /// <summary>
            ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
            /// </summary>
            /// <filterpriority>2</filterpriority>
            public void Dispose()
            {
                ThreadLocalLogger.Value = null;
            }

            #endregion
        }

        #endregion
    }
}