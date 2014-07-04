// CqlSharp - CqlSharp
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

namespace CqlSharp.Logging
{
    internal class TraceLoggerFactory : LoggerFactory<TraceLogger>
    {
        #region Implementation of ILoggerFactory

        public TraceLoggerFactory() : base("Trace")
        {
        }

        #endregion
    }

    /// <summary>
    /// Logger that logs to debug output
    /// </summary>
    internal class TraceLogger : ILogger
    {
        private const int EVENT_ID = 0; // TODO: maybe change to anything more useful later
        private readonly string _name;
        private TraceSource _source { get; set; }

        public TraceLogger(string name)
        {
            _name = name;
            _source = new TraceSource(name) {Switch = new SourceSwitch(name) {Level = SourceLevels.All}};
        }

        #region Implementation of ILogger

        public void LogVerbose(Guid traceId, string format, params object[] values)
        {
            _source.TraceEvent(TraceEventType.Verbose, EVENT_ID, format, values);
        }

        public void LogQuery(Guid traceId, string format, params object[] values)
        {
            _source.TraceEvent(TraceEventType.Verbose, EVENT_ID, format, values);
        }

        public void LogInfo(Guid traceId, string format, params object[] values)
        {
            _source.TraceEvent(TraceEventType.Information, EVENT_ID, format, values);
        }

        public void LogWarning(Guid traceId, string format, params object[] values)
        {
            _source.TraceEvent(TraceEventType.Warning, EVENT_ID, format, values);
        }

        public void LogError(Guid traceId, string format, params object[] values)
        {
            _source.TraceEvent(TraceEventType.Error, EVENT_ID, format, values);
        }

        public void LogCritical(Guid traceId, string format, params object[] values)
        {
            _source.TraceEvent(TraceEventType.Critical, EVENT_ID, format, values);
        }

        #endregion
    }
}