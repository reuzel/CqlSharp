// CqlSharp - CqlSharp.NLog
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
using CqlSharp.Logging;
using NLog;

namespace CqlSharp.NLog
{
    /// <summary>
    /// Nlog logging
    /// </summary>
    public class NLogLogger : ILogger
    {
        private readonly Logger _logger;

        public NLogLogger(string name)
        {
            _logger = LogManager.GetLogger(name);
        }

        public void LogCritical(Guid traceId, string format, params object[] values)
        {
            if(_logger.IsFatalEnabled)
                _logger.Fatal(traceId + " - " + format, values);
        }

        public void LogError(Guid traceId, string format, params object[] values)
        {
            if(_logger.IsErrorEnabled)
                _logger.Error(traceId + " - " + format, values);
        }

        public void LogInfo(Guid traceId, string format, params object[] values)
        {
            if(_logger.IsInfoEnabled)
                _logger.Info(traceId + " - " + format, values);
        }

        public void LogQuery(Guid traceId, string format, params object[] values)
        {
            if(_logger.IsDebugEnabled)
                _logger.Debug(traceId + " - " + format, values);
        }

        public void LogVerbose(Guid traceId, string format, params object[] values)
        {
            if(_logger.IsTraceEnabled)
                _logger.Trace(traceId + " - " + format, values);
        }

        public void LogWarning(Guid traceId, string format, params object[] values)
        {
            if(_logger.IsWarnEnabled)
                _logger.Warn(traceId + " - " + format, values);
        }
    }
}