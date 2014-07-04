using System;
using System.Diagnostics;

namespace CqlSharp.Logging
{
    internal class TraceLoggerFactory : LoggerFactory<TraceLogger>
    {
        #region Implementation of ILoggerFactory

        public TraceLoggerFactory() : base("Trace") { }

        #endregion
    }
    /// <summary>
    ///   Logger that logs to debug output
    /// </summary>
    internal class TraceLogger : ILogger
    {
        private const int EVENT_ID = 0; // TODO: maybe change to anything more useful later
        private readonly string _name;
        private TraceSource _source { get; set; }

        public TraceLogger(string name)
        {
            _name = name;
            _source = new TraceSource(name) { Switch = new SourceSwitch(name) { Level = SourceLevels.All } };
        }

        #region Implementation of ILogger

        public void LogVerbose(Guid traceId, string format, params object[] values)
        {
            Trace.CorrelationManager.ActivityId = traceId;
            _source.TraceEvent(TraceEventType.Verbose, EVENT_ID, format, values);
        }

        public void LogQuery(Guid traceId, string format, params object[] values)
        {
            Trace.CorrelationManager.ActivityId = traceId;
            _source.TraceEvent(TraceEventType.Verbose, EVENT_ID, format, values);
        }

        public void LogInfo(Guid traceId, string format, params object[] values)
        {
            Trace.CorrelationManager.ActivityId = traceId;
            _source.TraceEvent(TraceEventType.Information, EVENT_ID, format, values);
        }

        public void LogWarning(Guid traceId, string format, params object[] values)
        {
            Trace.CorrelationManager.ActivityId = traceId;
            _source.TraceEvent(TraceEventType.Warning, EVENT_ID, format, values);
        }

        public void LogError(Guid traceId, string format, params object[] values)
        {
            Trace.CorrelationManager.ActivityId = traceId;
            _source.TraceEvent(TraceEventType.Error, EVENT_ID, format, values);
        }

        public void LogCritical(Guid traceId, string format, params object[] values)
        {
            Trace.CorrelationManager.ActivityId = traceId;
            _source.TraceEvent(TraceEventType.Critical, EVENT_ID, format, values);
        }

        #endregion
    }
}
