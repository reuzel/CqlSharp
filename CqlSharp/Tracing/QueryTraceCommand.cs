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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CqlSharp.Threading;

namespace CqlSharp.Tracing
{
    /// <summary>
    /// Helper command to fetch Cassandra tracing information
    /// </summary>
    public class QueryTraceCommand
    {
        private readonly CqlConnection _connection;
        private readonly Guid _tracingId;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryTraceCommand" /> class.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="tracingId"> The tracing id. </param>
        public QueryTraceCommand(CqlConnection connection, Guid tracingId)
        {
            _connection = connection;
            _tracingId = tracingId;
        }

        /// <summary>
        /// Gets the trace session async.
        /// </summary>
        /// <returns> TracingSession if any, null otherwise </returns>
        public async Task<TracingSession> GetTraceSessionAsync(CancellationToken token)
        {
            TracingSession session;
            var sessionCmd = new CqlCommand(_connection,
                                            "select * from system_traces.sessions where session_id=" +
                                            _tracingId + ";", CqlConsistency.One);
            using(
                CqlDataReader<TracingSession> reader =
                    await sessionCmd.ExecuteReaderAsync<TracingSession>(token).AutoConfigureAwait())
            {
                if (await reader.ReadAsync(token).AutoConfigureAwait())
                    session = reader.Current;
                else
                    return null;
            }

            var eventsCmd = new CqlCommand(_connection,
                                           "select * from system_traces.events where session_id=" +
                                           _tracingId + ";", CqlConsistency.One);
            using(
                CqlDataReader<TracingEvent> reader =
                    await eventsCmd.ExecuteReaderAsync<TracingEvent>(token).AutoConfigureAwait())
            {
                var events = new List<TracingEvent>(reader.Count);
                while(await reader.ReadAsync().AutoConfigureAwait())
                    events.Add(reader.Current);

                session.Events = events;
            }

            return session;
        }

        /// <summary>
        /// Gets the trace session.
        /// </summary>
        /// <remarks>
        /// Convenience wrapper around GetTraceSessionAsync
        /// </remarks>
        /// <returns> TracingSession if any, null otherwise </returns>
        public TracingSession GetTraceSession()
        {
            return Scheduler.RunSynchronously(() => GetTraceSessionAsync(CancellationToken.None));
        }
    }
}