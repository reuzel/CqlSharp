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
using System.Threading;

namespace CqlSharp.Logging
{
    /// <summary>
    ///   Represents a logger for a single logical flow through the library
    /// </summary>
    internal struct Logger
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
        ///   The log level
        /// </summary>
        private readonly LogLevel _logLevel;

        /// <summary>
        ///   reference to the actual logger implementation
        /// </summary>
        private readonly ILogger _logger;

        /// <summary>
        ///   The id of the active trace
        /// </summary>
        private readonly Guid _traceId;

        /// <summary>
        ///   Initializes a new instance of the <see cref="Logger" /> class.
        /// </summary>
        /// <param name="logger"> The logger. </param>
        /// <param name="logLevel">Minimum level to use for logging</param>
        public Logger(ILogger logger, LogLevel logLevel)
        {
            _logger = logger;
            _traceId = FastGuid.NewGuid();
            _logLevel = logLevel;
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
            if (_logLevel == LogLevel.Verbose)
                _logger.LogVerbose(_traceId, format, values);
        }

        public void LogQuery(string format, params object[] values)
        {
            if (_logLevel <= LogLevel.Query)
                _logger.LogQuery(_traceId, format, values);
        }

        public void LogInfo(string format, params object[] values)
        {
            if (_logLevel <= LogLevel.Info)
                _logger.LogInfo(_traceId, format, values);
        }

        public void LogWarning(string format, params object[] values)
        {
            if (_logLevel <= LogLevel.Warning)
                _logger.LogWarning(_traceId, format, values);
        }

        public void LogError(string format, params object[] values)
        {
            if (_logLevel <= LogLevel.Error)
                _logger.LogError(_traceId, format, values);
        }

        public void LogCritical(string format, params object[] values)
        {
            if (_logLevel <= LogLevel.Critical)
                _logger.LogCritical(_traceId, format, values);
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
                ThreadLocalLogger.Value = default(Logger);
            }

            #endregion
        }

        #endregion
    }
}