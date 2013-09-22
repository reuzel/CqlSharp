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
using CqlSharp.Memory;
using CqlSharp.Network;
using CqlSharp.Network.Partition;
using CqlSharp.Protocol;
using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp
{
    /// <summary>
    ///   A Cql query
    /// </summary>
    public class CqlCommand : DbCommand
    {
        private CancellationTokenSource _cancelTokenSource;
        private string _commandText;
        private int _commandTimeout;
        private CommandType _commandType;
        private CqlConnection _connection;
        private CqlParameterCollection _parameters;
        private PartitionKey _partitionKey;
        private bool _prepared;
        private string _query;
        private ICqlQueryResult _queryResult;

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlCommand" /> class.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="cql"> The CQL. </param>
        /// <param name="level"> The level. </param>
        public CqlCommand(CqlConnection connection, string cql, CqlConsistency level)
        {
            _connection = connection;
            _commandText = cql;
            Consistency = level;
            _prepared = false;
            Load = 1;
            UseBuffering = connection.Config.UseBuffering;
            _commandType = CommandType.Text;
            _commandTimeout = 30;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlCommand" /> class. Uses a default consistency level One
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="cql"> The CQL. </param>
        /// <param name="level"> The level. </param>
        public CqlCommand(IDbConnection connection, string cql, CqlConsistency level)
            : this((CqlConnection)connection, cql, level)
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
            : this((CqlConnection)connection, cql, CqlConsistency.One)
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
            : this((CqlConnection)connection, "", CqlConsistency.One)
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
        /// Gets or sets a value indicating whether to use local serial for Compare-And-Set (CAS) write operations.
        /// </summary>
        /// <value>
        /// <c>true</c> if use local serial for Compare-And-Set (CAS)  write operations; otherwise, <c>false</c>.
        /// </value>
        public bool UseCASLocalSerial { get; set; }

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
        ///   Gets a value indicating whether this command is prepared.
        /// </summary>
        /// <value> <c>true</c> if this command is prepared; otherwise, <c>false</c> . </value>
        public bool IsPrepared
        {
            get { return _prepared; }
        }

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

        #region CommandText

        /// <summary>
        ///   Gets or sets the text command to run against the data source.
        /// </summary>
        /// <returns> The text command to execute. The default value is an empty string (""). </returns>
        public override string CommandText
        {
            get { return _commandText; }
            set
            {
                _commandText = value;
                _query = null;
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

        #endregion

        #region Parameters

        /// <summary>
        /// Gets a value indicating whether this command has any parameters.
        /// </summary>
        /// <value>
        ///   <c>true</c> if  this command has any parameters; otherwise, <c>false</c>.
        /// </value>
        public bool HasParameters
        {
            get { return _parameters != null && _parameters.Count > 0; }
        }

        /// <summary>
        ///   Gets the parameters that need to be set before executing a prepared query
        /// </summary>
        /// <value> The parameters. </value>
        /// <exception cref="CqlException">Parameters are available only after a query has been prepared</exception>
        public new CqlParameterCollection Parameters
        {
            get
            {
                if (_parameters == null)
                    _parameters = new CqlParameterCollection();

                return _parameters;
            }
        }

        /// <summary>
        ///   Gets the collection of <see cref="T:System.Data.Common.DbParameter" /> objects.
        /// </summary>
        /// <returns> The parameters of the SQL statement or stored procedure. </returns>
        protected override DbParameterCollection DbParameterCollection
        {
            get { return Parameters; }
        }

        /// <summary>
        ///   Creates a new instance of a <see cref="T:CqlSharp.CqlParameter" /> object.
        /// </summary>
        /// <returns> A <see cref="T:CqlSharp.CqlParameter" /> object. </returns>
        public new CqlParameter CreateParameter()
        {
            return new CqlParameter();
        }

        /// <summary>
        ///   Creates a new instance of a <see cref="T:System.Data.Common.DbParameter" /> object.
        /// </summary>
        /// <returns> A <see cref="T:System.Data.Common.DbParameter" /> object. </returns>
        protected override DbParameter CreateDbParameter()
        {
            return CreateParameter();
        }

        #endregion


        /// <summary>
        ///   Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
        /// </summary>
        /// <returns> The time (in seconds) to wait for the command to execute. The default value is 30 seconds. </returns>
        /// <exception cref="System.NotSupportedException"></exception>
        public override int CommandTimeout
        {
            get { return _commandTimeout; }
            set { _commandTimeout = value; }
        }

        /// <summary>
        ///   Indicates or specifies how the <see cref="P:System.Data.IDbCommand.CommandText" /> property is interpreted.
        /// </summary>
        /// <returns> One of the <see cref="T:System.Data.CommandType" /> values. The default is Text. </returns>
        /// <exception cref="System.ArgumentException">Only Text and TableDirect commands are supported</exception>
        public override CommandType CommandType
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
        ///   Gets the last query result. This may provide more information on the effects of a
        ///   query, especially for NonQueries.
        /// </summary>
        /// <value> The last query result. </value>
        public ICqlQueryResult LastQueryResult
        {
            get { return _queryResult; }
        }

        /// <summary>
        ///   Gets or sets the <see cref="T:CqlSharp.CqlConnection" /> used by this <see cref="T:CqlSharp.CqlCommand" />.
        /// </summary>
        /// <returns> The connection to the data source. </returns>
        public new CqlConnection Connection
        {
            get { return _connection; }
            set { _connection = value; }
        }

        /// <summary>
        ///   Gets or sets the <see cref="T:System.Data.Common.DbConnection" /> used by this <see
        ///    cref="T:System.Data.Common.DbCommand" />.
        /// </summary>
        /// <returns> The connection to the data source. </returns>
        protected override DbConnection DbConnection
        {
            get { return Connection; }
            set { Connection = (CqlConnection)value; }
        }

        /// <summary>
        ///   Gets or sets the <see cref="P:System.Data.Common.DbCommand.DbTransaction" /> within which this <see
        ///    cref="T:System.Data.Common.DbCommand" /> object executes.
        /// </summary>
        /// <returns> The transaction within which a Command object of a .NET Framework data provider executes. The default value is a null reference (Nothing in Visual Basic). </returns>
        /// <exception cref="System.NotSupportedException"></exception>
        protected override DbTransaction DbTransaction
        {
            get { return Transaction; }
            set { Transaction = (CqlBatchTransaction)value; }
        }

        /// <summary>
        /// Gets or sets the <see cref="T:CqlSharp.CqlBatchTransaction" /> within which this <see cref="T:CqlSharp.CqlCommand" /> object executes.
        /// </summary>
        /// <returns>The transaction within which a Command object of a .NET Framework data provider executes. The default value is a null reference (Nothing in Visual Basic).</returns>
        public new CqlBatchTransaction Transaction { get; set; }

        /// <summary>
        ///   Gets or sets a value indicating whether the command object should be visible in a customized interface control.
        /// </summary>
        /// <returns> true, if the command object should be visible in a control; otherwise false. The default is true. </returns>
        public override bool DesignTimeVisible { get; set; }


        /// <summary>
        ///   Gets or sets how command results are applied to the <see cref="T:System.Data.DataRow" /> when used by the Update method of a <see
        ///    cref="T:System.Data.Common.DbDataAdapter" />.
        /// </summary>
        /// <returns> One of the <see cref="T:System.Data.UpdateRowSource" /> values. The default is Both unless the command is automatically generated. Then the default is None. </returns>
        public override UpdateRowSource UpdatedRowSource { get; set; }

        /// <summary>
        ///   Enables rows to be returned/queried in batches of the give page size.
        /// </summary>
        /// <value> The size of the page. If PageSize &lt; = 0; no paging will be applied, otherwise the number indicates the number of rows to fetch in each batch. Default = 0 </value>
        public int PageSize { get; set; }

        /// <summary>
        ///   Gets or sets the state used to fetch a next page of results. This value is set/reset by a 
        ///   DataReader instance
        /// </summary>
        /// <value> The state of the paging. </value>
        internal byte[] PagingState { get; set; }

        /// <summary>
        ///   Executes the query, and returns the value of the first column of the first row.
        /// </summary>
        /// <returns> </returns>
        public override object ExecuteScalar()
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
        ///   Cancels the execution of this command.
        /// </summary>
        public override void Cancel()
        {
            if (_cancelTokenSource != null)
                _cancelTokenSource.Cancel();
        }

        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        protected override void Dispose(bool disposing)
        {
            if (disposing && _cancelTokenSource != null)
            {
                _cancelTokenSource.Dispose();
                _cancelTokenSource = null;
            }

            base.Dispose(disposing);
        }

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
                                     ? new CancellationTokenSource(CommandTimeout * 1000)
                                     : new CancellationTokenSource();
            return _cancelTokenSource.Token;
        }

        /// <summary>
        ///   Executes the reader.
        /// </summary>
        /// <returns> </returns>
        public new CqlDataReader ExecuteReader()
        {
            return ExecuteReader(CommandBehavior.Default);
        }

        /// <summary>
        ///   Executes the query.
        /// </summary>
        /// <param name="behavior"> The behavior. </param>
        /// <returns> </returns>
        public new CqlDataReader ExecuteReader(CommandBehavior behavior)
        {
            try
            {
                var token = SetupCancellationToken();
                return ExecuteReaderAsync(behavior, token).Result;
            }
            catch (AggregateException aex)
            {
                throw aex.InnerException;
            }
        }

        /// <summary>
        ///   Executes the query.
        /// </summary>
        /// <param name="behavior"> An instance of <see cref="T:System.Data.CommandBehavior" /> . </param>
        /// <returns> CqlDataReader that can be used to read the results </returns>
        /// <remarks>
        ///   Invokes ExecuteReader
        /// </remarks>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return ExecuteReader(behavior);
        }

        /// <summary>
        ///   Executes the query asynchronous.
        /// </summary>
        /// <returns> </returns>
        public new Task<CqlDataReader> ExecuteReaderAsync()
        {
            return ExecuteReaderAsync(CommandBehavior.Default, CancellationToken.None);
        }

        /// <summary>
        ///   Executes the reader asynchronous.
        /// </summary>
        /// <param name="cancellationToken"> The cancellation token. </param>
        /// <returns> </returns>
        public new Task<CqlDataReader> ExecuteReaderAsync(CancellationToken cancellationToken)
        {
            return ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);
        }

        /// <summary>
        ///   Executes the query asynchronous.
        /// </summary>
        /// <param name="behavior"> The behavior. </param>
        /// <returns> </returns>
        public new Task<CqlDataReader> ExecuteReaderAsync(CommandBehavior behavior)
        {
            return ExecuteReaderAsync(behavior, CancellationToken.None);
        }

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior,
                                                                             CancellationToken cancellationToken)
        {
            return await ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        ///   Executes the reader asynchronous.
        /// </summary>
        /// <param name="behavior"> The behavior. </param>
        /// <param name="cancellationToken"> The cancellation token. </param>
        /// <returns> </returns>
        public new async Task<CqlDataReader> ExecuteReaderAsync(CommandBehavior behavior,
                                                                CancellationToken cancellationToken)
        {
            var result = await ExecuteReaderAsyncInternal(behavior, cancellationToken);

            var reader = new CqlDataReader(this, result,
                                           behavior.HasFlag(CommandBehavior.CloseConnection) ? _connection : null);
            _queryResult = reader;
            return reader;
        }

        /// <summary>
        ///   Executes the query async.
        /// </summary>
        /// <typeparam name="T"> class representing the rows returned </typeparam>
        /// <returns> </returns>
        public Task<CqlDataReader<T>> ExecuteReaderAsync<T>() where T : class, new()
        {
            return ExecuteReaderAsync<T>(CommandBehavior.Default, CancellationToken.None);
        }


        /// <summary>
        ///   Executes the query async.
        /// </summary>
        /// <typeparam name="T"> class representing the rows returned </typeparam>
        /// <param name="behavior"> The behavior. </param>
        /// <returns> </returns>
        public Task<CqlDataReader<T>> ExecuteReaderAsync<T>(CommandBehavior behavior) where T : class, new()
        {
            return ExecuteReaderAsync<T>(behavior, CancellationToken.None);
        }

        /// <summary>
        ///   Executes the query async.
        /// </summary>
        /// <typeparam name="T"> class representing the rows returned </typeparam>
        /// <param name="cancellationToken"> The cancellation token. </param>
        /// <returns> </returns>
        public Task<CqlDataReader<T>> ExecuteReaderAsync<T>(CancellationToken cancellationToken) where T : class, new()
        {
            return ExecuteReaderAsync<T>(CommandBehavior.Default, cancellationToken);
        }

        /// <summary>
        ///   Executes the query async.
        /// </summary>
        /// <typeparam name="T"> class representing the rows returned </typeparam>
        /// <param name="behavior"> The behavior. </param>
        /// <param name="cancellationToken"> The cancellation token. </param>
        /// <returns> </returns>
        public async Task<CqlDataReader<T>> ExecuteReaderAsync<T>(CommandBehavior behavior,
                                                                  CancellationToken cancellationToken)
            where T : class, new()
        {
            var result = await ExecuteReaderAsyncInternal(behavior, cancellationToken);

            var reader = new CqlDataReader<T>(this, result,
                                              behavior.HasFlag(CommandBehavior.CloseConnection) ? _connection : null);
            _queryResult = reader;
            return reader;
        }

        /// <summary>
        ///   Executes the query.
        /// </summary>
        /// <typeparam name="T"> class representing the rows returned </typeparam>
        /// <returns> CqlDataReader that can be used to read the results </returns>
        /// <remarks>
        ///   Utility wrapper around ExecuteReaderAsync
        /// </remarks>
        public CqlDataReader<T> ExecuteReader<T>() where T : class, new()
        {
            return ExecuteReader<T>(CommandBehavior.Default);
        }

        /// <summary>
        ///   Executes the query.
        /// </summary>
        /// <typeparam name="T"> class representing the rows returned </typeparam>
        /// <param name="behavior"> The behavior. </param>
        /// <returns> CqlDataReader that can be used to read the results </returns>
        /// <remarks>
        ///   Utility wrapper around ExecuteReaderAsync
        /// </remarks>
        public CqlDataReader<T> ExecuteReader<T>(CommandBehavior behavior) where T : class, new()
        {
            try
            {
                var token = SetupCancellationToken();
                return ExecuteReaderAsync<T>(behavior, token).Result;
            }
            catch (AggregateException aex)
            {
                throw aex.InnerException;
            }
        }

        /// <summary>
        ///   Executes the select (read) operation asynchronous.
        /// </summary>
        /// <param name="behavior"> The behavior. </param>
        /// <param name="cancellationToken"> The cancellation token. </param>
        /// <returns> </returns>
        /// <exception cref="System.ArgumentException">Command behavior not supported;behavior</exception>
        internal async Task<ResultFrame> ExecuteReaderAsyncInternal(CommandBehavior behavior,
                                                                    CancellationToken cancellationToken)
        {
            //clear last result
            _queryResult = null;

            if (behavior.HasFlag(CommandBehavior.SequentialAccess))
                UseBuffering = false;

            if (behavior.HasFlag(CommandBehavior.KeyInfo) ||
                behavior.HasFlag(CommandBehavior.SchemaOnly) ||
                behavior.HasFlag(CommandBehavior.SingleRow))
                throw new ArgumentException("Command behavior not supported", "behavior");

            var logger = _connection.LoggerManager.GetLogger("CqlSharp.CqlCommand.ExecuteReader");

            logger.LogVerbose("Waiting on Throttle");

            //wait until allowed
            _connection.Throttle.Wait(cancellationToken);

            try
            {
                logger.LogVerbose("Start executing query");

                ResultFrame result =
                    await RunWithRetry(SendQueryAsync, logger, cancellationToken).ConfigureAwait(false);

                if (result.CqlResultType != CqlResultType.Rows)
                {
                    var ex = new CqlException("Can not create a DataReader for non-select query.");
                    logger.LogError("Error executing reader: {0}", ex);
                    throw ex;
                }

                logger.LogQuery("Query {0} returned {1} results", Query, result.Count);

                //copy metadata from prepared query cache if necessary
                if (_prepared && result.ResultMetaData.NoMetaData)
                    result.ResultMetaData.CopyColumnsFrom(_connection.PreparedQueryCache[Query].ResultMetaData);

                return result;
            }
            finally
            {
                _connection.Throttle.Release();
            }
        }

        /// <summary>
        ///   Executes the query, and returns the value of the first column of the first row.
        /// </summary>
        /// <param name="token"> The cancellation token. </param>
        /// <returns> </returns>
        /// <exception cref="CqlException">Execute Scalar Query yield no results</exception>
        public override async Task<object> ExecuteScalarAsync(CancellationToken token)
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
        ///   Executes a SQL statement against a connection object.
        /// </summary>
        /// <returns> The number of rows affected. </returns>
        public override int ExecuteNonQuery()
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
        ///   Executes the non query asynchronous.
        /// </summary>
        /// <param name="token"> The token. </param>
        /// <returns> </returns>
        public override async Task<int> ExecuteNonQueryAsync(CancellationToken token)
        {
            //clear last result
            _queryResult = null;

            //check token first
            token.ThrowIfCancellationRequested();

            var logger = _connection.LoggerManager.GetLogger("CqlSharp.CqlCommand.ExecuteNonQuery");

            //attempt to add to the current transaction if any
            if (EnlistInTransactionIfAny())
            {
                logger.LogVerbose("Query {0} was enlisted with a CqlBatchTransaction", Query);
                return 1;
            }

            logger.LogVerbose("Waiting on Throttle");

            //wait until allowed
            _connection.Throttle.Wait(token);

            try
            {
                logger.LogVerbose("Start executing query");

                ResultFrame result = await RunWithRetry(SendQueryAsync, logger, token).ConfigureAwait(false);
                switch (result.CqlResultType)
                {
                    case CqlResultType.Rows:
                        var reader = new CqlDataReader(this, result, null);
                        _queryResult = reader;
                        logger.LogQuery("Query {0} returned {1} results", Query, reader.Count);
                        return -1;

                    case CqlResultType.Void:
                        logger.LogQuery("Query {0} executed succesfully", Query);
                        _queryResult = new CqlVoid { TracingId = result.TracingId };
                        return 1;

                    case CqlResultType.SchemaChange:
                        logger.LogQuery("Query {0} resulted in {1}.{2} {3}", Query, result.Keyspace, result.Table,
                                        result.Change);

                        _queryResult = new CqlSchemaChange
                                           {
                                               TracingId = result.TracingId,
                                               Keyspace = result.Keyspace,
                                               Table = result.Table,
                                               Change = result.Change
                                           };

                        return -1;

                    case CqlResultType.SetKeyspace:
                        logger.LogQuery("Query {0} resulted in keyspace set to {1}", Query, result.Keyspace);
                        _queryResult = new CqlSetKeyspace
                                           {
                                               TracingId = result.TracingId,
                                               Keyspace = result.Keyspace
                                           };

                        return -1;

                    default:
                        throw new CqlException("Unexpected type of result received");
                }
            }
            finally
            {
                _connection.Throttle.Release();
            }
        }

        #region Batch

        /// <summary>
        /// Executes a batch
        /// </summary>
        internal void ExecuteBatch()
        {
            try
            {
                var token = SetupCancellationToken();
                ExecuteBatchAsync(token).Wait();
            }
            catch (AggregateException aex)
            {
                throw aex.InnerException;
            }
        }

        /// <summary>
        /// Executes the batch asynchronous.
        /// </summary>
        /// <param name="token">The token.</param>
        /// <returns></returns>
        /// <exception cref="CqlException">Unexpected type of result received</exception>
        internal async Task ExecuteBatchAsync(CancellationToken token)
        {
            //check token first
            token.ThrowIfCancellationRequested();

            var logger = _connection.LoggerManager.GetLogger("CqlSharp.CqlBatchTransaction.Commit");

            logger.LogVerbose("Waiting on Throttle");

            //wait until allowed
            _connection.Throttle.Wait();

            try
            {
                //continue?
                token.ThrowIfCancellationRequested();

                logger.LogVerbose("Start executing batch");

                ResultFrame result = await RunWithRetry(SendBatchAsync, logger, token).ConfigureAwait(false);
                switch (result.CqlResultType)
                {
                    case CqlResultType.Void:
                        logger.LogQuery("Bath executed succesfully");
                        _queryResult = new CqlVoid { TracingId = result.TracingId };
                        break;

                    default:
                        throw new CqlException("Unexpected type of result received");
                }
            }
            finally
            {
                _connection.Throttle.Release();
            }
        }

        #endregion


        #region Prepare

        /// <summary>
        ///   Creates a prepared (or compiled) version of the command on the data source.
        /// </summary>
        public override void Prepare()
        {
            var logger = _connection.LoggerManager.GetLogger("CqlSharp.CqlCommand.Prepare");

            ResultFrame result;
            if (_connection.PreparedQueryCache.TryGetValue(Query, out result))
            {
                FinalizePrepare(result, true);
                logger.LogVerbose("Prepared query {0} from Cache", Query);
            }
            else
            {
                try
                {
                    var token = SetupCancellationToken();
                    PrepareAsyncInternal(token, logger).Wait();
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
        }

        /// <summary>
        ///   Prepares the command asynchronous.
        /// </summary>
        /// <returns> </returns>
        public Task PrepareAsync()
        {
            return PrepareAsync(CancellationToken.None);
        }

        /// <summary>
        ///   Prepares the command asynchronous.
        /// </summary>
        /// <param name="token"> The cancellation token. </param>
        /// <returns> </returns>
        public Task PrepareAsync(CancellationToken token)
        {
            var logger = _connection.LoggerManager.GetLogger("CqlSharp.CqlCommand.Prepare");

            ResultFrame result;
            if (_connection.PreparedQueryCache.TryGetValue(Query, out result))
            {
                FinalizePrepare(result, true);

                logger.LogVerbose("Prepared query {0} from Cache", Query);

                //return a already completed task
                return TaskCache.CompletedTask;
            }

            //nothing from cache, go the long way
            return PrepareAsyncInternal(token, logger);
        }

        /// <summary>
        ///   Finalizes a prepare operation.
        /// </summary>
        /// <param name="result"> The result. </param>
        /// <param name="fromCache"> if set to <c>true</c> [from cache]. </param>
        private void FinalizePrepare(ResultFrame result, bool fromCache)
        {
            _queryResult = new CqlPrepared { TracingId = result.TracingId, FromCache = fromCache };

            //set as prepared
            _prepared = true;

            //set parameters collection
            if (_parameters == null || _parameters.Count == 0)
                _parameters = new CqlParameterCollection(result.QueryMetaData);

            //fix the parameter collection (if not done so already)
            _parameters.Fixate();
        }

        /// <summary>
        ///   Prepares the command asynchronous by sending an actual prepare request to the server.
        /// </summary>
        /// <param name="token"> The cancellation token. </param>
        /// <param name="logger"> </param>
        /// <returns> </returns>
        private async Task PrepareAsyncInternal(CancellationToken token, Logger logger)
        {
            //continue?
            token.ThrowIfCancellationRequested();

            logger.LogVerbose("Waiting on Throttle");

            //wait until allowed
            _connection.Throttle.Wait();

            try
            {
                //continue?
                token.ThrowIfCancellationRequested();

                logger.LogVerbose("Start executing prepare query");

                ResultFrame result = await RunWithRetry(SendPrepareAsync, logger, token).ConfigureAwait(false);

                FinalizePrepare(result, false);

                logger.LogQuery("Prepared query {0}", Query);
            }
            finally
            {
                _connection.Throttle.Release();
            }
        }



        #endregion


        #region Frame submission

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
                        var useFrame = new QueryFrame("use '" + _connection.Database + "';", CqlConsistency.One, null);
                        var result = await connection.SendRequestAsync(useFrame, logger, 1, false, token) as ResultFrame;
                        if (result == null || result.CqlResultType != CqlResultType.SetKeyspace)
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
        ///   Prepares the query async on the given connection.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="logger"> The logger. </param>
        /// <param name="token"> The token. </param>
        /// <returns> </returns>
        /// <exception cref="CqlException">Unexpected frame received  + response.OpCode</exception>
        private async Task<ResultFrame> SendPrepareAsync(Connection connection, Logger logger,
                                                         CancellationToken token)
        {
            //create prepare frame
            var query = new PrepareFrame(Query);

            //update frame with tracing option if requested
            if (EnableTracing)
                query.Flags |= FrameFlags.Tracing;

            logger.LogVerbose("Sending prepare {0} using {1}", Query, connection);

            //send prepare request
            using (
                Frame response = await connection.SendRequestAsync(query, logger, 1, false, token).ConfigureAwait(false)
                )
            {
                var result = response as ResultFrame;
                if (result == null)
                {
                    throw new CqlException("Unexpected frame received " + response.OpCode);
                }

                _connection.PreparedQueryCache[Query] = result;
                connection.Node.PreparedQueryIds[Query] = result.PreparedQueryId;

                return result;
            }
        }


        /// <summary>
        ///   Executes the query async on the given connection
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="logger"> The logger. </param>
        /// <param name="token"> The token. </param>
        /// <returns> </returns>
        /// <exception cref="CqlException">Unexpected frame received</exception>
        private async Task<ResultFrame> SendQueryAsync(Connection connection, Logger logger,
                                                       CancellationToken token)
        {
            QueryFrameBase queryFrame;
            if (_prepared)
            {
                byte[] queryId;
                if (!connection.Node.PreparedQueryIds.TryGetValue(Query, out queryId))
                {
                    ResultFrame prepareResult =
                        await SendPrepareAsync(connection, logger, token).ConfigureAwait(false);

                    queryId = prepareResult.PreparedQueryId;
                }

                queryFrame = new ExecuteFrame(queryId, Consistency, Parameters.Values);

                logger.LogVerbose("Sending execute {0} using {1}", Query, connection);
            }
            else
            {
                byte[][] values = _parameters != null && _parameters.Count > 0 ? _parameters.Values : null;
                queryFrame = new QueryFrame(Query, Consistency, values);
                logger.LogVerbose("Sending query {0} using {1}", Query, connection);
            }

            //set page size (if any)
            if (PageSize > 0)
                queryFrame.PageSize = PageSize;

            //set paging state
            if (PagingState != null)
            {
                logger.LogVerbose("Query is to fetch a next page of data");
                queryFrame.PagingState = PagingState;
            }

            //set local serial
            if (UseCASLocalSerial)
            {
                logger.LogVerbose("Using LocalSerial consistency for CAS prepare and propose");
                queryFrame.SerialConsistency = SerialConsistency.LocalSerial;
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

        /// <summary>
        /// Adds this command to the current batch transaction.
        /// </summary>
        private bool EnlistInTransactionIfAny()
        {
            if (Transaction != null)
            {
                var batchedCommand = new BatchFrame.BatchedCommand
                                         {
                                             IsPrepared = IsPrepared,
                                             CqlQuery = Query,
                                             ParameterValues = HasParameters ? Parameters.Values : null
                                         };

                Transaction.Commands.Add(batchedCommand);

                return true;
            }

            return false;
        }

        /// <summary>
        ///   Executes the query async on the given connection
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="logger"> The logger. </param>
        /// <param name="token"> The token. </param>
        /// <returns> </returns>
        /// <exception cref="CqlException">Unexpected frame received</exception>
        private async Task<ResultFrame> SendBatchAsync(Connection connection, Logger logger,
                                                       CancellationToken token)
        {
            var batchFrame = new BatchFrame(Transaction.BatchType, Transaction.Consistency);
            foreach (var command in Transaction.Commands)
            {
                if (command.IsPrepared)
                {
                    byte[] queryId;
                    if (!connection.Node.PreparedQueryIds.TryGetValue(command.CqlQuery, out queryId))
                    {
                        ResultFrame prepareResult =
                            await SendPrepareAsync(connection, logger, token).ConfigureAwait(false);

                        queryId = prepareResult.PreparedQueryId;
                    }
                    command.QueryId = queryId;
                }

                batchFrame.Commands.Add(command);
            }

            //update frame with tracing option if requested
            if (EnableTracing)
                batchFrame.Flags |= FrameFlags.Tracing;

            logger.LogVerbose("Sending batch command using {0}", connection);

            Frame response =
                await connection.SendRequestAsync(batchFrame, logger, Load, false, token).ConfigureAwait(false);

            var result = response as ResultFrame;
            if (result != null)
            {
                return result;
            }

            //unexpected frame received!
            response.Dispose();
            throw new CqlException("Unexpected frame received " + response.OpCode);
        }

        #endregion


    }
}