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

using CqlSharp.Logging;
using CqlSharp.Network;
using CqlSharp.Network.Partition;
using CqlSharp.Protocol;
using System;
using System.Threading.Tasks;

namespace CqlSharp
{
    /// <summary>
    ///   A Cql query
    /// </summary>
    public class CqlCommand
    {
        private readonly CqlConnection _connection;
        private readonly string _cql;
        private readonly CqlConsistency _level;
        private CqlParameterCollection _parameters;
        private PartitionKey _partitionKey;
        private bool _prepared;

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlCommand" /> class.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="cql"> The CQL. </param>
        /// <param name="level"> The level. </param>
        public CqlCommand(CqlConnection connection, string cql, CqlConsistency level)
        {
            _connection = connection;
            _cql = cql;
            _level = level;
            _prepared = false;
            Load = 1;
            UseBuffering = connection.Config.UseBuffering;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlCommand" /> class. Uses a default consistency level One
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="cql"> The CQL. </param>
        public CqlCommand(CqlConnection connection, string cql)
            : this(connection, cql, CqlConsistency.One)
        {
        }

        /// <summary>
        ///   Gets the parameters that need to be set before executing a prepared query
        /// </summary>
        /// <value> The parameters. </value>
        /// <exception cref="CqlException">Parameters are available only after a query has been prepared</exception>
        public CqlParameterCollection Parameters
        {
            get
            {
                if (_parameters == null)
                    throw new CqlException("Parameters are available only after a query has been prepared");

                return _parameters;
            }
        }

        /// <summary>
        ///   Gets or sets a value indicating whether to use response buffering.
        /// </summary>
        /// <value> <c>true</c> if buffering should be used; otherwise, <c>false</c> . </value>
        public bool UseBuffering { get; set; }

        /// <summary>
        ///   Gets or sets a value indicating whether tracing enabled should be enabled.
        /// </summary>
        /// <value> <c>true</c> if tracing enabled; otherwise, <c>false</c> . </value>
        public bool EnableTracing { get; set; }

        /// <summary>
        ///   Gets or sets a value indicating whether the query is allowed to use different connection, than the default connection from the used CqlConnection.
        /// </summary>
        /// <value> <c>true</c> if parallel connections are allowed to be used; otherwise, <c>false</c> . </value>
        public bool UseParallelConnections { get; set; }

        /// <summary>
        ///   Indication of the load this query generates (e.g. the number of expected returned rows). Used by connection stratagies for balancing
        ///   queries over connections.
        /// </summary>
        /// <value> The load. Defaults to 1 </value>
        public int Load { get; set; }

        /// <summary>
        ///   The partition key, used to route queries to corresponding nodes in the cluster
        /// </summary>
        /// <value> The partition key. </value>
        public PartitionKey PartitionKey
        {
            get
            {
                if (_partitionKey == null)
                    _partitionKey = new PartitionKey();

                return _partitionKey;
            }
        }

        /// <summary>
        ///   Executes the query async.
        /// </summary>
        /// <returns> CqlDataReader that can be used to read the results </returns>
        public async Task<CqlDataReader> ExecuteReaderAsync()
        {
            var logger = _connection.LoggerManager.GetLogger("CqlSharp.CqlCommand.ExecuteReader");

            logger.LogVerbose("Waiting on Throttle");

            //wait until allowed
            _connection.Throttle.Wait();

            try
            {
                //capture state
                QueryExecutionState state = CaptureState();

                logger.LogVerbose("State captured, start executing query");

                ResultFrame result = await RunWithRetry(ExecuteInternalAsync, state, logger).ConfigureAwait(false);

                if (result.ResultOpcode != ResultOpcode.Rows)
                {
                    var ex = new CqlException("Can not create a DataReader for non-select query.");
                    logger.LogError("Error executing reader: {0}", ex);
                    throw ex;
                }

                var reader = new CqlDataReader(result);

                logger.LogQuery("Query {0} returned {1} results", _cql, reader.Count);

                return reader;
            }
            finally
            {
                _connection.Throttle.Release();
            }
        }

        /// <summary>
        ///   Executes the query.
        /// </summary>
        /// <remarks>
        ///   Utility wrapper around ExecuteReaderAsync
        /// </remarks>
        /// <returns> CqlDataReader that can be used to read the results </returns>
        public CqlDataReader ExecuteReader()
        {
            try
            {
                return ExecuteReaderAsync().Result;
            }
            catch (AggregateException aex)
            {
                throw aex.InnerException;
            }
        }


        /// <summary>
        ///   Executes the query async.
        /// </summary>
        /// <typeparam name="T"> class representing the rows returned </typeparam>
        /// <returns> </returns>
        public async Task<CqlDataReader<T>> ExecuteReaderAsync<T>() where T : class, new()
        {
            var logger = _connection.LoggerManager.GetLogger("CqlSharp.CqlCommand.ExecuteReader");

            logger.LogVerbose("Waiting on Throttle");

            //wait until allowed
            _connection.Throttle.Wait();

            try
            {
                //capture current command state
                QueryExecutionState state = CaptureState();

                logger.LogVerbose("State captured, start executing query");

                ResultFrame result = await RunWithRetry(ExecuteInternalAsync, state, logger).ConfigureAwait(false);

                if (result.ResultOpcode != ResultOpcode.Rows)
                {
                    var ex = new CqlException("Can not create a DataReader for non-select query.");
                    logger.LogError("Error executing reader: {0}", ex);
                    throw ex;
                }
                var reader = new CqlDataReader<T>(result);

                logger.LogQuery("Query {0} returned {1} results", _cql, reader.Count);

                return reader;
            }
            finally
            {
                _connection.Throttle.Release();
            }
        }

        /// <summary>
        ///   Executes the query.
        /// </summary>
        /// <remarks>
        ///   Utility wrapper around ExecuteReaderAsync
        /// </remarks>
        /// <typeparam name="T"> class representing the rows returned </typeparam>
        /// <returns> CqlDataReader that can be used to read the results </returns>
        public CqlDataReader<T> ExecuteReader<T>() where T : class, new()
        {
            try
            {
                return ExecuteReaderAsync<T>().Result;
            }
            catch (AggregateException aex)
            {
                throw aex.InnerException;
            }
        }

        /// <summary>
        ///   Executes the query, and returns the value of the first column of the first row.
        /// </summary>
        /// <returns> </returns>
        public async Task<object> ExecuteScalarAsync()
        {
            object result;

            using (var reader = await ExecuteReaderAsync().ConfigureAwait(false))
            {
                if (await reader.ReadAsync().ConfigureAwait(false))
                {
                    result = reader[0];
                }
                else
                {
                    throw new CqlException("Execute Scalar Query yield no results");
                }
            }

            return result;
        }

        /// <summary>
        ///   Executes the query, and returns the value of the first column of the first row.
        /// </summary>
        /// <returns> </returns>
        public object ExecuteScalar()
        {
            try
            {
                return ExecuteScalarAsync().Result;
            }
            catch (AggregateException aex)
            {
                throw aex.InnerException;
            }
        }

        /// <summary>
        ///   Executes the non-query async.
        /// </summary>
        /// <returns> A ICqlQueryResult of type rows, Void, SchemaChange or SetKeySpace </returns>
        /// <exception cref="CqlException">Unexpected type of result received</exception>
        public async Task<ICqlQueryResult> ExecuteNonQueryAsync()
        {
            var logger = _connection.LoggerManager.GetLogger("CqlSharp.CqlCommand.ExecuteNonQuery");

            logger.LogVerbose("Waiting on Throttle");

            //wait until allowed
            _connection.Throttle.Wait();

            try
            {
                //capture current command state
                QueryExecutionState state = CaptureState();

                logger.LogVerbose("State captured, start executing query");

                ResultFrame result = await RunWithRetry(ExecuteInternalAsync, state, logger).ConfigureAwait(false);
                switch (result.ResultOpcode)
                {
                    case ResultOpcode.Rows:
                        var reader = new CqlDataReader(result);
                        logger.LogQuery("Query {0} returned {1} results", _cql, reader.Count);
                        return reader;

                    case ResultOpcode.Void:
                        logger.LogQuery("Query {0} executed succesfully", _cql);
                        return new CqlVoid { TracingId = result.TracingId };

                    case ResultOpcode.SchemaChange:
                        logger.LogQuery("Query {0} resulted in {1}.{2} {3}", _cql, result.Keyspace, result.Table, result.Change);
                        return new CqlSchemaChange
                                   {
                                       TracingId = result.TracingId,
                                       Keyspace = result.Keyspace,
                                       Table = result.Table,
                                       Change = result.Change
                                   };

                    case ResultOpcode.SetKeyspace:
                        logger.LogQuery("Query {0} resulted in keyspace set to {1}", _cql, result.Keyspace);
                        return new CqlSetKeyspace
                                   {
                                       TracingId = result.TracingId,
                                       Keyspace = result.Keyspace
                                   };

                    default:
                        throw new CqlException("Unexpected type of result received");
                }
            }
            finally
            {
                _connection.Throttle.Release();
            }
        }

        /// <summary>
        ///   Executes the non-query.
        /// </summary>
        /// <remarks>
        ///   Utility wrapper around ExecuteNonQueryAsync
        /// </remarks>
        /// <returns> A ICqlQueryResult of type rows, Void, SchemaChange or SetKeySpace </returns>
        /// <exception cref="CqlException">Unexpected type of result received</exception>
        public ICqlQueryResult ExecuteNonQuery()
        {
            try
            {
                return ExecuteNonQueryAsync().Result;
            }
            catch (AggregateException aex)
            {
                throw aex.InnerException;
            }
        }


        /// <summary>
        ///   Prepares the query async.
        /// </summary>
        /// <returns> </returns>
        public async Task PrepareAsync()
        {
            var logger = _connection.LoggerManager.GetLogger("CqlSharp.CqlCommand.Prepare");

            logger.LogVerbose("Waiting on Throttle");

            //wait until allowed
            _connection.Throttle.Wait();
            try
            {
                //capture state
                var state = CaptureState();

                logger.LogVerbose("State captured, start executing query");

                await RunWithRetry(PrepareInternalAsync, state, logger).ConfigureAwait(false);

                logger.LogQuery("Prepared query {0}", _cql);
            }
            finally
            {
                _connection.Throttle.Release();
            }
        }

        /// <summary>
        ///   Captures the state.
        /// </summary>
        /// <returns> </returns>
        private QueryExecutionState CaptureState()
        {
            var state = new QueryExecutionState
                            {
                                Values = _parameters == null ? null : _parameters.Values,
                                TracingEnabled = EnableTracing,
                                UseBuffering = UseBuffering,
                                UseParallelConnections = UseParallelConnections,
                                PartitionKey = PartitionKey != null ? PartitionKey.Copy() : null,
                                Load = Load
                            };
            return state;
        }

        /// <summary>
        ///   Prepares the query
        /// </summary>
        /// <remarks>
        ///   Utility wrapper around PrepareAsync
        /// </remarks>
        /// <returns> A ICqlQueryResult of type rows, Void, SchemaChange or SetKeySpace </returns>
        /// <exception cref="CqlException">Unexpected type of result received</exception>
        public void Prepare()
        {
            try
            {
                PrepareAsync().Wait();
            }
            catch (AggregateException aex)
            {
                throw aex.InnerException;
            }
        }


        /// <summary>
        /// Runs the given function, and retries it on a new connection when I/O or node errors occur
        /// </summary>
        /// <param name="executeFunc">The function to execute.</param>
        /// <param name="state">The state.</param>
        /// <param name="logger">The logger.</param>
        /// <returns></returns>
        /// <exception cref="CqlException">Failed to return query result after max amount of attempts</exception>
        private async Task<ResultFrame> RunWithRetry(
            Func<Connection, QueryExecutionState, Logger, Task<ResultFrame>> executeFunc, QueryExecutionState state, Logger logger)
        {
            int attempts = _connection.Config.MaxQueryRetries;

            //keep trying until faulted
            for (int attempt = 0; attempt < attempts; attempt++)
            {
                //check if this query is to run using its own connection
                bool newConn = state.UseParallelConnections;

                //get me a connection

                Connection connection;
                using (logger.ThreadBinding())
                {
                    connection = _connection.GetConnection(newConn, state.PartitionKey);

                }
                try
                {


                    ResultFrame result = await executeFunc(connection, state, logger).ConfigureAwait(false);

                    if (newConn)
                    {
                        using (logger.ThreadBinding())
                            _connection.ReturnConnection(connection);
                    }

                    return result;
                }
                catch (ProtocolException pex)
                {
                    if (attempt == attempts - 1)
                    {
                        logger.LogError("Query failed after {0} attempts with error {1}", attempts, pex);
                        throw;
                    }

                    switch (pex.Code)
                    {
                        case ErrorCode.IsBootstrapping:
                        case ErrorCode.Overloaded:
                            //IO or node status related error, go for rerun
                            logger.LogWarning("Query to {0} failed because server returned {1}, going for retry", connection, pex.Code.ToString());
                            continue;
                        default:
                            logger.LogWarning("Query failed with {0} error: {1}", pex.Code.ToString(), pex.Message);
                            //some other Cql error (syntax ok?), quit
                            throw;
                    }
                }
                catch (Exception ex)
                {
                    if (attempt == attempts - 1)
                    {
                        logger.LogError("Query failed after {0} attempts with error {1}", attempts, ex);
                        throw;
                    }

                    //connection probable collapsed, go an try again
                    logger.LogWarning("Query to {0} failed, going for retry. {1}", connection, ex);
                }
            }

            throw new CqlException("Failed to return query result after max amount of attempts");

        }

        /// <summary>
        /// Prepares the query async on the given connection. Returns immediatly if the query is already
        /// prepared.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="state">captured state</param>
        /// <param name="logger">The logger.</param>
        /// <returns></returns>
        /// <exception cref="CqlException">Unexpected frame received  + response.OpCode</exception>
        /// <exception cref="System.Exception">Unexpected frame received  + response.OpCode</exception>
        private async Task<ResultFrame> PrepareInternalAsync(Connection connection, QueryExecutionState state, Logger logger)
        {
            //check if already prepared for this connection
            ResultFrame result;

            var prepareResults = _connection.GetPrepareResultsFor(_cql);
            if (!prepareResults.TryGetValue(connection.Address, out result))
            {
                //create prepare frame
                var query = new PrepareFrame(_cql);

                //update frame with tracing option if requested
                if (state.TracingEnabled)
                    query.Flags |= FrameFlags.Tracing;

                logger.LogVerbose("No prepare results available. Sending prepare {0} using {1}", _cql, connection);

                //send prepare request
                Frame response = await connection.SendRequestAsync(query, logger).ConfigureAwait(false);

                result = response as ResultFrame;
                if (result == null)
                    throw new CqlException("Unexpected frame received " + response.OpCode);

                prepareResults[connection.Address] = result;
            }
            else
            {
                logger.LogVerbose("Reusing cached preparation results");
            }

            //set as prepared
            _prepared = true;

            //set parameters collection if not done so before
            if (_parameters == null)
                _parameters = new CqlParameterCollection(result.Schema);

            return result;
        }


        /// <summary>
        /// Executes the query async on the given connection
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="state">The state.</param>
        /// <param name="logger">The logger.</param>
        /// <returns></returns>
        /// <exception cref="CqlException">Unexpected frame received</exception>
        private async Task<ResultFrame> ExecuteInternalAsync(Connection connection, QueryExecutionState state, Logger logger)
        {
            Frame query;
            if (_prepared)
            {
                ResultFrame prepareResult = await PrepareInternalAsync(connection, state, logger).ConfigureAwait(false);
                query = new ExecuteFrame(prepareResult.PreparedQueryId, _level, state.Values);
                logger.LogVerbose("Sending execute {0} using {1}", _cql, connection);
            }
            else
            {
                query = new QueryFrame(_cql, _level);
                logger.LogVerbose("Sending query {0} using {1}", _cql, connection);
            }

            //update frame with tracing option if requested
            if (state.TracingEnabled)
                query.Flags |= FrameFlags.Tracing;

            Frame response = await connection.SendRequestAsync(query, logger, state.Load).ConfigureAwait(false);

            var result = response as ResultFrame;
            if (result != null)
            {
                //read all the data into a buffer, if requested
                if (state.UseBuffering)
                {
                    logger.LogVerbose("Buffering used, reading all data");
                    await result.BufferDataAsync().ConfigureAwait(false);
                }

                return result;
            }

            throw new CqlException("Unexpected frame received " + response.OpCode);
        }
    }
}