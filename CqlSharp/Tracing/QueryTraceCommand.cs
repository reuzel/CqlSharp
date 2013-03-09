using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CqlSharp.Tracing
{
    /// <summary>
    /// Helper command to fetch Cassandra tracing information
    /// </summary>
    public class QueryTraceCommand
    {
        private readonly CqlConnection _connection;
        private Guid _tracingId;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryTraceCommand" /> class.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="tracingId">The tracing id.</param>
        public QueryTraceCommand(CqlConnection connection, Guid tracingId)
        {
            _connection = connection;
            _tracingId = tracingId;
        }

        /// <summary>
        /// Gets the trace session async.
        /// </summary>
        /// <returns>TracingSession if any, null otherwise</returns>
        public async Task<TracingSession> GetTraceSessionAsync()
        {
            TracingSession session = null;
            var sessionCmd = new CqlCommand(_connection,
                                            "select * from system_traces.sessions where session_id='" +
                                            _tracingId.ToString() + "';", CqlConsistency.One);
            using (CqlDataReader<TracingSession> reader = await sessionCmd.ExecuteReaderAsync<TracingSession>())
            {
                if (await reader.ReadAsync())
                {
                    session = reader.Current;
                }
                else
                    return null;
            }

            var eventsCmd = new CqlCommand(_connection,
                                           "select * from system_traces.events where session_id='" +
                                           _tracingId.ToString() + "';", CqlConsistency.One);
            using (CqlDataReader<TracingEvent> reader = await eventsCmd.ExecuteReaderAsync<TracingEvent>())
            {
                var events = new List<TracingEvent>(reader.Count);
                while (await reader.ReadAsync())
                {
                    events.Add(reader.Current);
                }

                session.Events = events;
            }

            return session;
        }

        /// <summary>
        /// Gets the trace session.
        /// </summary>
        /// <remarks>Convenience wrapper around GetTraceSessionAsync</remarks>
        /// <returns>TracingSession if any, null otherwise</returns>
        public TracingSession GetTraceSession()
        {
            try
            {
                return GetTraceSessionAsync().Result;
            }
            catch (AggregateException aex)
            {
                throw aex.InnerException;
            }
        }
    }
}