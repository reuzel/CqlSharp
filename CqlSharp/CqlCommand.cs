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
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using CqlSharp.Config;
using CqlSharp.Logging;
using CqlSharp.Network;
using CqlSharp.Network.Partition;
using CqlSharp.Protocol;

namespace CqlSharp
{
    /// <summary>
    ///   A Cql query
    /// </summary>
    public class CqlCommand : IDbCommand
    {
        private CancellationTokenSource _cancelTokenSource;
        private string _commandText;
        private CommandType _commandType;
        private CqlConnection _connection;
        private CqlParameterCreationOption _paramCreation;
        private CqlParameterCollection _parameters;
        private PartitionKey _partitionKey;
        private bool _prepared;
        private string _query;

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlCommand" /> class.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="cql"> The CQL. </param>
        /// <param name="level"> The level. </param>
        public CqlCommand(CqlConnection connection, string cql, CqlConsistency level)
        {
            _connection = connection;
            CommandText = cql;
            Consistency = level;
            _prepared = false;
            Load = 1;
            UseBuffering = connection.Config.UseBuffering;
            _commandType = CommandType.Text;
            CommandTimeout = 30;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlCommand" /> class. Uses a default consistency level One
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="cql"> The CQL. </param>
        /// <param name="level"> The level. </param>
        public CqlCommand(IDbConnection connection, string cql, CqlConsistency level)
            : this((CqlConnection) connection, cql, level)
        {
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
        ///   Initializes a new instance of the <see cref="CqlCommand" /> class. Uses a default consistency level One
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="cql"> The CQL. </param>
        public CqlCommand(IDbConnection connection, string cql)
            : this((CqlConnection) connection, cql, CqlConsistency.One)
        {
        }


        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlCommand" /> class.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        public CqlCommand(CqlConnection connection)
            : this(connection, "", CqlConsistency.One)
        {
        }

        // <summary>
        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlCommand" /> class.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        public CqlCommand(IDbConnection connection)
            : this((CqlConnection) connection, "", CqlConsistency.One)
        {
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlCommand" /> class.
        /// </summary>
        public CqlCommand()
            : this(null, "", CqlConsistency.One)
        {
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
        ///   Indication of the load this query generates (e.g. the number of expected returned rows). Used by connection stratagies for balancing
        ///   queries over connections.
        /// </summary>
        /// <value> The load. Defaults to 1 </value>
        public int Load { get; set; }

        /// <summary>
        ///   Gets or sets the consistency level to use with this command Defaults to CqlConsisteny.One.
        /// </summary>
        /// <value> The consistency. </value>
        public CqlConsistency Consistency { get; set; }

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
        ///   Gets the query.
        /// </summary>
        /// <value> The query. </value>
        /// <exception cref="System.NotSupportedException">Only Text and TableDirect queries are supported</exception>
        private string Query
        {
            get
            {
                if (_query == null)
                {
                    switch (CommandType)
                    {
                        case CommandType.Text:
                            _query = CommandText;
                            break;
                        case CommandType.TableDirect:
                            _query = "select * from '" + CommandText.Trim() + "';";
                            break;
                        default:
                            throw new NotSupportedException("Only Text and TableDirect queries are supported");
                    }
                }

                return _query;
            }
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
                    _parameters = new CqlParameterCollection();

                return _parameters;
            }
        }

        #region IDbCommand Members

        /// <summary>
        ///   Gets or sets the text command to run against the data source.
        /// </summary>
        /// <returns> The text command to execute. The default value is an empty string (""). </returns>
        public string CommandText
        {
            get { return _commandText; }
            set
            {
                _commandText = value;
                _query = null;
            }
        }

        /// <summary>
        ///   Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
        /// </summary>
        /// <returns> The time (in seconds) to wait for the command to execute. The default value is 30 seconds. </returns>
        /// <exception cref="System.NotSupportedException"></exception>
        public int CommandTimeout { get; set; }

        /// <summary>
        ///   Indicates or specifies how the <see cref="P:System.Data.IDbCommand.CommandText" /> property is interpreted.
        /// </summary>
        /// <returns> One of the <see cref="T:System.Data.CommandType" /> values. The default is Text. </returns>
        /// <exception cref="System.ArgumentException">Only Text and TableDirect commands are supported</exception>
        public CommandType CommandType
        {
            get { return _commandType; }
            set
            {
                if (value == CommandType.StoredProcedure)
                    throw new ArgumentException("Only Text and TableDirect commands are supported");

                if (value != _commandType)
                {
                    _commandType = value;
                    _query = null;
                }
            }
        }

        /// <summary>
        ///   Gets or sets the <see cref="T:System.Data.IDbConnection" /> used by this instance of the <see
        ///    cref="T:System.Data.IDbCommand" />.
        /// </summary>
        /// <returns> The connection to the data source. </returns>
        public IDbConnection Connection
        {
            get { return _connection; }
            set { _connection = (CqlConnection) value; }
        }


        /// <summary>
        ///   Gets or sets the transaction within which the Command object of a .NET Framework data provider executes.
        /// </summary>
        /// <returns> the Command object of a .NET Framework data provider executes. The default value is null. </returns>
        /// <exception cref="System.NotSupportedException"></exception>
        IDbTransaction IDbCommand.Transaction
        {
            get { throw new NotSupportedException(); }
            set { throw new NotSupportedException(); }
        }

        /// <summary>
        ///   Gets or sets how command results are applied to the <see cref="T:System.Data.DataRow" /> when used by the <see
        ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> method of a <see
        ///    cref="T:System.Data.Common.DbDataAdapter" />.
        /// </summary>
        /// <returns> One of the <see cref="T:System.Data.UpdateRowSource" /> values. The default is Both unless the command is automatically generated. Then the default is None. </returns>
        UpdateRowSource IDbCommand.UpdatedRowSource { get; set; }

        /// <summary>
        ///   Gets the <see cref="T:System.Data.IDataParameterCollection" />.
        /// </summary>
        /// <returns> The parameters of the SQL statement or stored procedure. </returns>
        /// <filterpriority>2</filterpriority>
        IDataParameterCollection IDbCommand.Parameters
        {
            get { return Parameters; }
        }

        /// <summary>
        ///   Creates a new instance of an <see cref="T:System.Data.IDbDataParameter" /> object.
        /// </summary>
        /// <returns> An IDbDataParameter object. </returns>
        public IDbDataParameter CreateParameter()
        {
            return new CqlParameter();
        }

        /// <summary>
        ///   Executes the <see cref="P:System.Data.IDbCommand.CommandText" /> against the <see
        ///    cref="P:System.Data.IDbCommand.Connection" /> and builds an <see cref="T:System.Data.IDataReader" />.
        /// </summary>
        /// <returns> An <see cref="T:System.Data.IDataReader" /> object. </returns>
        /// <filterpriority>2</filterpriority>
        IDataReader IDbCommand.ExecuteReader()
        {
            return ExecuteReader();
        }

        /// <summary>
        ///   Executes the <see cref="P:System.Data.IDbCommand.CommandText" /> against the <see
        ///    cref="P:System.Data.IDbCommand.Connection" />, and builds an <see cref="T:System.Data.IDataReader" /> using one of the <see
        ///    cref="T:System.Data.CommandBehavior" /> values.
        /// </summary>
        /// <param name="behavior"> One of the <see cref="T:System.Data.CommandBehavior" /> values. </param>
        /// <returns> An <see cref="T:System.Data.IDataReader" /> object. </returns>
        /// <exception cref="System.ArgumentException">Command behavior not supported;behavior</exception>
        public IDataReader ExecuteReader(CommandBehavior behavior)
        {
            if (behavior.HasFlag(CommandBehavior.SequentialAccess))
                UseBuffering = false;

            if (behavior.HasFlag(CommandBehavior.KeyInfo) ||
                behavior.HasFlag(CommandBehavior.SchemaOnly) ||
                behavior.HasFlag(CommandBehavior.SingleRow))
                throw new ArgumentException("Command behavior not supported", "behavior");

            IDataReader reader = ExecuteReader();

            if (behavior.HasFlag(CommandBehavior.CloseConnection))
                _connection.Close();

            return reader;
        }

        /// <summary>
        ///   Executes the query, and returns the value of the first column of the first row.
        /// </summary>
        /// <returns> </returns>
        public object ExecuteScalar()
        {
            try
            {
                var token = SetupCancellationToken();
                return ExecuteScalarAsync(token).Result;
            }
            catch (AggregateException aex)
            {
                throw aex.InnerException;
            }
        }

        /// <summary>
        ///   Executes the non-query. Will return 1 always as Cql does not return information on the amount
        ///   of rows updated.
        /// </summary>
        /// <returns> </returns>
        int IDbCommand.ExecuteNonQuery()
        {
            ExecuteNonQuery();
            return 1;
        }

        /// <summary>
        ///   Cancels the execution of this command.
        /// </summary>
        public void Cancel()
        {
            if (_cancelTokenSource != null)
                _cancelTokenSource.Cancel();
        }

        /// <summary>
        ///   Creates a prepared (or compiled) version of the command on the data source.
        /// </summary>
        void IDbCommand.Prepare()
        {
            Prepare(CqlParameterCreationOption.None);
        }

        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        void IDisposable.Dispose()
        {
            if (_cancelTokenSource != null)
            {
                _cancelTokenSource.Dispose();
                _cancelTokenSource = null;
            }
        }

        #endregion

        /// <summary>
        ///   Setups the cancellation token.
        /// </summary>
        /// <returns> </returns>
        private CancellationToken SetupCancellationToken()
        {
            //dispose any old token
            if (_cancelTokenSource != null)
                _cancelTokenSource.Dispose();

            //setup new token
            _cancelTokenSource = CommandTimeout > 0
                                     ? new CancellationTokenSource(CommandTimeout*1000)
                                     : new CancellationTokenSource();
            return _cancelTokenSource.Token;
        }

        /// <summary>
        ///   Executes the query async.
        /// </summary>
        /// <returns> CqlDataReader that can be used to read the results </returns>
        public Task<CqlDataReader> ExecuteReaderAsync()
        {
            return ExecuteReaderAsync(CancellationToken.None);
        }

        /// <summary>
        ///   Executes the query async.
        /// </summary>
        /// <param name="token"> The cancelletion token. </param>
        /// <returns> CqlDataReader that can be used to read the results </returns>
        public async Task<CqlDataReader> ExecuteReaderAsync(CancellationToken token)
        {
            var logger = _connection.LoggerManager.GetLogger("CqlSharp.CqlCommand.ExecuteReader");

            logger.LogVerbose("Waiting on Throttle");

            //wait until allowed
            _connection.Throttle.Wait();

            try
            {
                logger.LogVerbose("Start executing query");

                ResultFrame result = await RunWithRetry(ExecuteInternalAsync, logger, token).ConfigureAwait(false);

                if (result.ResultOpcode != ResultOpcode.Rows)
                {
                    var ex = new CqlException("Can not create a DataReader for non-select query.");
                    logger.LogError("Error executing reader: {0}", ex);
                    throw ex;
                }

                var reader = new CqlDataReader(result);

                logger.LogQuery("Query {0} returned {1} results", Query, reader.Count);

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
                var token = SetupCancellationToken();
                return ExecuteReaderAsync(token).Result;
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
        public Task<CqlDataReader<T>> ExecuteReaderAsync<T>() where T : class, new()
        {
            return ExecuteReaderAsync<T>(CancellationToken.None);
        }

        /// <summary>
        ///   Executes the query async.
        /// </summary>
        /// <typeparam name="T"> class representing the rows returned </typeparam>
        /// <param name="token"> The cancellation token. </param>
        /// <returns> </returns>
        public async Task<CqlDataReader<T>> ExecuteReaderAsync<T>(CancellationToken token) where T : class, new()
        {
            var logger = _connection.LoggerManager.GetLogger("CqlSharp.CqlCommand.ExecuteReader");

            logger.LogVerbose("Waiting on Throttle");

            //wait until allowed
            _connection.Throttle.Wait();

            try
            {
                logger.LogVerbose("Start executing query");

                ResultFrame result = await RunWithRetry(ExecuteInternalAsync, logger, token).ConfigureAwait(false);

                if (result.ResultOpcode != ResultOpcode.Rows)
                {
                    var ex = new CqlException("Can not create a DataReader for non-select query.");
                    logger.LogError("Error executing reader: {0}", ex);
                    throw ex;
                }
                var reader = new CqlDataReader<T>(result);

                logger.LogQuery("Query {0} returned {1} results", Query, reader.Count);

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
                var token = SetupCancellationToken();
                return ExecuteReaderAsync<T>(token).Result;
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
        /// <exception cref="CqlException">Execute Scalar Query yield no results</exception>
        public Task<object> ExecuteScalarAsync()
        {
            return ExecuteScalarAsync(CancellationToken.None);
        }

        /// <summary>
        ///   Executes the query, and returns the value of the first column of the first row.
        /// </summary>
        /// <param name="token"> The cancellation token. </param>
        /// <returns> </returns>
        /// <exception cref="CqlException">Execute Scalar Query yield no results</exception>
        public async Task<object> ExecuteScalarAsync(CancellationToken token)
        {
            object result;

            using (var reader = await ExecuteReaderAsync(token).ConfigureAwait(false))
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
        ///   Executes the non-query async.
        /// </summary>
        /// <returns> A ICqlQueryResult of type rows, Void, SchemaChange or SetKeySpace </returns>
        /// <exception cref="CqlException">Unexpected type of result received</exception>
        public Task<ICqlQueryResult> ExecuteNonQueryAsync()
        {
            return ExecuteNonQueryAsync(CancellationToken.None);
        }


        /// <summary>
        ///   Executes the non-query async.
        /// </summary>
        /// <param name="token"> The token. </param>
        /// <returns> A ICqlQueryResult of type rows, Void, SchemaChange or SetKeySpace </returns>
        /// <exception cref="CqlException">Unexpected type of result received</exception>
        public async Task<ICqlQueryResult> ExecuteNonQueryAsync(CancellationToken token)
        {
            //check token first
            token.ThrowIfCancellationRequested();

            var logger = _connection.LoggerManager.GetLogger("CqlSharp.CqlCommand.ExecuteNonQuery");

            logger.LogVerbose("Waiting on Throttle");

            //wait until allowed
            _connection.Throttle.Wait();

            //continue?
            token.ThrowIfCancellationRequested();

            try
            {
                logger.LogVerbose("Start executing query");

                ResultFrame result = await RunWithRetry(ExecuteInternalAsync, logger, token).ConfigureAwait(false);
                switch (result.ResultOpcode)
                {
                    case ResultOpcode.Rows:
                        var reader = new CqlDataReader(result);
                        logger.LogQuery("Query {0} returned {1} results", Query, reader.Count);
                        return reader;

                    case ResultOpcode.Void:
                        logger.LogQuery("Query {0} executed succesfully", Query);
                        return new CqlVoid {TracingId = result.TracingId};

                    case ResultOpcode.SchemaChange:
                        logger.LogQuery("Query {0} resulted in {1}.{2} {3}", Query, result.Keyspace, result.Table,
                                        result.Change);
                        return new CqlSchemaChange
                                   {
                                       TracingId = result.TracingId,
                                       Keyspace = result.Keyspace,
                                       Table = result.Table,
                                       Change = result.Change
                                   };

                    case ResultOpcode.SetKeyspace:
                        logger.LogQuery("Query {0} resulted in keyspace set to {1}", Query, result.Keyspace);
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
                var token = SetupCancellationToken();
                return ExecuteNonQueryAsync(token).Result;
            }
            catch (AggregateException aex)
            {
                throw aex.InnerException;
            }
        }


        /// <summary>
        ///   Prepares the query async.
        /// </summary>
        /// <param name="token"> The cancellation token. </param>
        /// <returns> </returns>
        public Task PrepareAsync(CancellationToken token)
        {
            return PrepareAsync(CqlParameterCreationOption.Column, token);
        }

        /// <summary>
        ///   Prepares the query async.
        /// </summary>
        /// <param name="paramCreation"> The parameter creation option. </param>
        /// <returns> </returns>
        public Task PrepareAsync(CqlParameterCreationOption paramCreation = CqlParameterCreationOption.Column)
        {
            return PrepareAsync(paramCreation, CancellationToken.None);
        }


        /// <summary>
        ///   Prepares the query async.
        /// </summary>
        /// <param name="token"> The cancellation token. </param>
        /// <param name="paramCreation"> The parameter creation option. </param>
        /// <returns> </returns>
        public async Task PrepareAsync(CqlParameterCreationOption paramCreation, CancellationToken token)
        {
            //continue?
            token.ThrowIfCancellationRequested();

            var logger = _connection.LoggerManager.GetLogger("CqlSharp.CqlCommand.Prepare");

            logger.LogVerbose("Waiting on Throttle");

            //wait until allowed
            _connection.Throttle.Wait();
            try
            {
                //continue?
                token.ThrowIfCancellationRequested();

                _paramCreation = paramCreation;

                logger.LogVerbose("State captured, start executing query");

                await RunWithRetry(PrepareInternalAsync, logger, token).ConfigureAwait(false);

                logger.LogQuery("Prepared query {0}", Query);
            }
            finally
            {
                _connection.Throttle.Release();
            }
        }

        /// <summary>
        ///   Prepares the query
        /// </summary>
        /// <remarks>
        ///   Utility wrapper around PrepareAsync
        /// </remarks>
        /// <returns> A ICqlQueryResult of type rows, Void, SchemaChange or SetKeySpace </returns>
        /// <exception cref="CqlException">Unexpected type of result received</exception>
        public void Prepare(CqlParameterCreationOption paramCreation)
        {
            try
            {
                var token = SetupCancellationToken();
                PrepareAsync(paramCreation, token).Wait();
            }
            catch (AggregateException aex)
            {
                throw aex.InnerException;
            }
            finally
            {
                _cancelTokenSource = null;
            }
        }

        /// <summary>
        ///   Creates a prepared (or compiled) version of the command on the data source.
        /// </summary>
        public void Prepare()
        {
            Prepare(CqlParameterCreationOption.Column);
        }


        /// <summary>
        ///   Runs the given function, and retries it on a new connection when I/O or node errors occur
        /// </summary>
        /// <param name="executeFunc"> The function to execute. </param>
        /// <param name="logger"> The logger. </param>
        /// <param name="token"> The token. </param>
        /// <returns> </returns>
        /// <exception cref="CqlException">Failed to return query result after max amount of attempts</exception>
        private async Task<ResultFrame> RunWithRetry(
            Func<Connection, Logger, CancellationToken, Task<ResultFrame>> executeFunc, Logger logger,
            CancellationToken token)
        {
            int attempts = _connection.Config.MaxQueryRetries;

            //keep trying until faulted
            for (int attempt = 0; attempt < attempts; attempt++)
            {
                //continue?
                token.ThrowIfCancellationRequested();

                //get me a connection
                Connection connection;
                using (logger.ThreadBinding())
                    connection = _connection.GetConnection(PartitionKey);

                try
                {
                    //set correct database if necessary
                    if (_connection.ProvidesExclusiveConnections &&
                        !string.IsNullOrWhiteSpace(_connection.Database) &&
                        !_connection.Database.Equals(connection.CurrentKeySpace))
                    {
                        var useFrame = new QueryFrame("use '" + _connection.Database + "';", CqlConsistency.One);
                        var result = await connection.SendRequestAsync(useFrame, logger, 1, false, token) as ResultFrame;
                        if (result == null || result.ResultOpcode != ResultOpcode.SetKeyspace)
                        {
                            if (result != null) result.Dispose();
                            throw new CqlException("Unexpected frame received");
                        }
                        //assume success
                        result.Dispose();
                    }

                    //execute
                    return await executeFunc(connection, logger, token).ConfigureAwait(false);
                }
                catch (TaskCanceledException)
                {
                    throw;
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
                            logger.LogWarning("Query to {0} failed because server returned {1}, going for retry",
                                              connection, pex.Code.ToString());
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
                        //out of attempts
                        logger.LogError("Query failed after {0} attempts with error {1}", attempts, ex);
                        throw;
                    }

                    if (_connection.Config.ConnectionStrategy == ConnectionStrategy.Exclusive)
                    {
                        //using exclusive connection strategy. If connection fails, do not recover
                        logger.LogError("Query failed on exclusive connection with error {0}", ex);
                        throw;
                    }

                    //connection probable collapsed, go an try again
                    logger.LogWarning("Query to {0} failed, going for retry. {1}", connection, ex);
                }
            }

            throw new CqlException("Failed to return query result after max amount of attempts");
        }

        /// <summary>
        ///   Prepares the query async on the given connection. Returns immediatly if the query is already
        ///   prepared.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="logger"> The logger. </param>
        /// <param name="token"> The token. </param>
        /// <returns> </returns>
        /// <exception cref="CqlException">Unexpected frame received  + response.OpCode</exception>
        /// <exception cref="System.Exception">Unexpected frame received  + response.OpCode</exception>
        private async Task<ResultFrame> PrepareInternalAsync(Connection connection, Logger logger,
                                                             CancellationToken token)
        {
            //check if already prepared for this connection
            ResultFrame result;

            var prepareResults = _connection.GetPrepareResultsFor(Query);
            if (!prepareResults.TryGetValue(connection.Address, out result))
            {
                //create prepare frame
                var query = new PrepareFrame(Query);

                //update frame with tracing option if requested
                if (EnableTracing)
                    query.Flags |= FrameFlags.Tracing;

                logger.LogVerbose("No prepare results available. Sending prepare {0} using {1}", Query, connection);

                //send prepare request
                Frame response = await connection.SendRequestAsync(query, logger, 1, false, token).ConfigureAwait(false);

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

            //set parameters collection
            if (_paramCreation != CqlParameterCreationOption.None && _parameters == null)
                _parameters = new CqlParameterCollection(result.Schema, _paramCreation);

            return result;
        }


        /// <summary>
        ///   Executes the query async on the given connection
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="logger"> The logger. </param>
        /// <param name="token"> The token. </param>
        /// <returns> </returns>
        /// <exception cref="CqlException">Unexpected frame received</exception>
        private async Task<ResultFrame> ExecuteInternalAsync(Connection connection, Logger logger,
                                                             CancellationToken token)
        {
            Frame queryFrame;
            if (_prepared)
            {
                ResultFrame prepareResult = await PrepareInternalAsync(connection, logger, token).ConfigureAwait(false);
                queryFrame = new ExecuteFrame(prepareResult.PreparedQueryId, Consistency,
                                              _parameters == null ? null : _parameters.Values);
                logger.LogVerbose("Sending execute {0} using {1}", Query, connection);
            }
            else
            {
                queryFrame = new QueryFrame(Query, Consistency);
                logger.LogVerbose("Sending query {0} using {1}", Query, connection);
            }

            //update frame with tracing option if requested
            if (EnableTracing)
                queryFrame.Flags |= FrameFlags.Tracing;

            Frame response =
                await connection.SendRequestAsync(queryFrame, logger, Load, false, token).ConfigureAwait(false);

            var result = response as ResultFrame;
            if (result != null)
            {
                //read all the data into a buffer, if requested
                if (UseBuffering)
                {
                    logger.LogVerbose("Buffering used, reading all data");
                    await result.BufferDataAsync().ConfigureAwait(false);
                }

                return result;
            }

            //unexpected frame received!
            response.Dispose();
            throw new CqlException("Unexpected frame received " + response.OpCode);
        }
    }
}