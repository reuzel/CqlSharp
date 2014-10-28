// CqlSharp.Linq - CqlSharp.Linq
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
using System.Data;

namespace CqlSharp.Linq
{
    /// <summary>
    ///   Provides access to the database underlying a context
    /// </summary>
    public class CqlDatabase : IDisposable
    {
        private readonly CqlContext _cqlContext;
        private CqlConnection _connection;
        private string _connectionString;
        private CqlBatchTransaction _currentTransaction;
        private bool _disposeConnection;
        private bool _disposeTransaction;

        internal CqlDatabase(CqlContext cqlContext)
        {
            _cqlContext = cqlContext;
            CommandTimeout = 10;
        }

        /// <summary>
        ///   Gets or sets the command timeout.
        /// </summary>
        /// <value> The command timeout. </value>
        public int? CommandTimeout { get; set; }

        /// <summary>
        ///   Gets the connection string.
        /// </summary>
        /// <value> The connection string. </value>
        public string ConnectionString
        {
            get
            {
                if (_connectionString == null)
                    _connectionString = _cqlContext.GetType().Name;

                return _connectionString;
            }

            set
            {
                if (_connection != null)
                    throw new CqlLinqException(
                        "Can not change database, as the connection of the context is already opened.");

                _connectionString = value;
            }
        }

        /// <summary>
        /// Gets or sets the keyspace (database) used for all queries.
        /// </summary>
        /// <value>
        /// The key space.
        /// </value>
        /// <remarks>
        /// Changing the keyspace can only be done if the underlying connection strategy
        /// provides exclusive connections.
        /// </remarks>
        public string Keyspace
        {
            get { return Connection.Database; }
            set
            {
                if (Connection.State != ConnectionState.Open)
                    Connection.Open();

                Connection.ChangeDatabase(value);
            }
        }

        /// <summary>
        ///   Gets or sets the log where executed CQL queries are written to
        /// </summary>
        /// <value> The log. </value>
        public Action<string> Log { get; set; }

        /// <summary>
        ///   Gets the connection.
        /// </summary>
        /// <value> The connection. </value>
        public CqlConnection Connection
        {
            get
            {
                if (_connection == null)
                    _connection = new CqlConnection(ConnectionString);

                return _connection;
            }
        }

        /// <summary>
        ///   Gets the current transaction.
        /// </summary>
        /// <value> The current transaction. </value>
        internal CqlBatchTransaction CurrentTransaction
        {
            get { return _currentTransaction; }
        }

        #region IDisposable Members

        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            if (_currentTransaction != null && _disposeTransaction)
                _currentTransaction.Dispose();

            if (_connection != null && _disposeConnection)
                _connection.Dispose();
        }

        #endregion

        /// <summary>
        ///   Logs the query.
        /// </summary>
        /// <param name="cql"> The CQL. </param>
        internal void LogQuery(string cql)
        {
            if (Log != null)
                Log(cql);
        }

        /// <summary>
        ///   Sets the connection.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="ownsConnection"> if set to <c>true</c> [owns connection]. </param>
        internal void SetConnection(CqlConnection connection, bool ownsConnection)
        {
            _connection = connection;
            _connectionString = _connection.ConnectionString;
            _disposeConnection = ownsConnection;
        }

        /// <summary>
        ///   Begins a transaction on which all operations in this context are executed.
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="System.InvalidOperationException">This context is already bound to an transaction</exception>
        public CqlBatchTransaction BeginTransaction()
        {
            if (_currentTransaction != null)
                throw new InvalidOperationException("This context is already bound to a transaction");

            var connection = Connection;
            if (connection.State == ConnectionState.Closed)
                connection.Open();

            _currentTransaction = new CqlContextBatchTransaction(this, connection);
            _disposeTransaction = true;

            return _currentTransaction;
        }

        /// <summary>
        ///   Makes operations in the context use the underlying transaction
        /// </summary>
        /// <param name="transaction"> The transaction. </param>
        public void UseTransaction(CqlBatchTransaction transaction)
        {
            if (transaction == null)
            {
                _currentTransaction = null;
                _disposeTransaction = false;
                return;
            }

            if (_currentTransaction != null)
                throw new InvalidOperationException("Context is already bound to a transaction.");

            if (transaction.Connection == null || transaction.Connection != Connection)
                throw new ArgumentException("Transaction is not using the same connection as this context",
                                            "transaction");

            _currentTransaction = transaction;
            _disposeTransaction = false;
        }
    }
}