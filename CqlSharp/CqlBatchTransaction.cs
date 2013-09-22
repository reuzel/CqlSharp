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

using CqlSharp.Memory;
using CqlSharp.Protocol;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp
{
    /// <summary>
    ///   A batched query. Any commands executed as part of a <see cref="CqlBatchTransaction" /> will be 
    ///   buffered and executed in a single batch when committed.
    /// </summary>
    public class CqlBatchTransaction : DbTransaction
    {
        private CqlCommand _batchCommand;
        private List<BatchFrame.BatchedCommand> _commands;

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlBatchTransaction" /> class.
        /// </summary>
        public CqlBatchTransaction()
        {
            _batchCommand = new CqlCommand { Transaction = this };
            _commands = new List<BatchFrame.BatchedCommand>();
            BatchType = CqlBatchType.Logged;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlBatchTransaction" /> class.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        public CqlBatchTransaction(CqlConnection connection)
            : this(connection, CqlBatchType.Logged, CqlConsistency.One)
        {
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlBatchTransaction" /> class.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="batchType"> Type of the batch. </param>
        public CqlBatchTransaction(CqlConnection connection, CqlBatchType batchType)
            : this(connection, batchType, CqlConsistency.One)
        {
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlBatchTransaction" /> class.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="batchType"> Type of the batch. </param>
        /// <param name="consistency"> The consistency. </param>
        public CqlBatchTransaction(CqlConnection connection, CqlBatchType batchType, CqlConsistency consistency)
        {
            _batchCommand = new CqlCommand(connection) { Consistency = consistency, Transaction = this };
            _commands = new List<BatchFrame.BatchedCommand>();

            BatchType = batchType;
        }

        /// <summary>
        ///   Gets or sets the wait time before terminating the attempt to execute a (batch) command and generating an error.
        /// </summary>
        /// <returns> The time (in seconds) to wait for the command to execute. The default value is 30 seconds. </returns>
        /// <exception cref="System.NotSupportedException"></exception>
        public int CommandTimeout
        {
            get
            {
                CheckIfDisposed();
                return _batchCommand.CommandTimeout;
            }
            set
            {
                CheckIfDisposed();
                _batchCommand.CommandTimeout = value;
            }
        }

        /// <summary>
        ///   Specifies the <see cref="T:System.Data.Common.DbConnection" /> object associated with the transaction.
        /// </summary>
        /// <returns> The <see cref="T:System.Data.Common.DbConnection" /> object associated with the transaction. </returns>
        protected override DbConnection DbConnection
        {
            get { return Connection; }
        }

        /// <summary>
        ///   Specifies the <see cref="T:CqlSharp.CqlConnection" /> object associated with the transaction.
        /// </summary>
        /// <returns> The <see cref="T:CqlSharp.CqlConnection" /> object associated with the transaction. </returns>
        public new CqlConnection Connection
        {
            get
            {
                CheckIfDisposed();
                return _batchCommand.Connection;
            }
            set
            {
                CheckIfDisposed();
                _batchCommand.Connection = value;
            }
        }

        /// <summary>
        ///   Specifies the <see cref="T:System.Data.IsolationLevel" /> for this transaction.
        /// </summary>
        /// <returns> The <see cref="T:System.Data.IsolationLevel" /> for this transaction. </returns>
        /// <filterpriority>1</filterpriority>
        public override IsolationLevel IsolationLevel
        {
            get { return IsolationLevel.Unspecified; }
        }

        /// <summary>
        ///   Gets or sets the type of the batch.
        /// </summary>
        /// <value> The type of the batch. </value>
        public CqlBatchType BatchType { get; set; }

        /// <summary>
        ///   Gets the commands.
        /// </summary>
        /// <value> The commands. </value>
        internal List<BatchFrame.BatchedCommand> Commands
        {
            get
            {
                CheckIfDisposed();

                if (_commands == null)
                    throw new InvalidOperationException("Transaction is rolled back");

                return _commands;
            }
        }

        /// <summary>
        ///   Gets or sets the consistency.
        /// </summary>
        /// <value> The consistency. </value>
        public CqlConsistency Consistency
        {
            get
            {
                CheckIfDisposed();
                return _batchCommand.Consistency;
            }
            set
            {
                CheckIfDisposed();
                _batchCommand.Consistency = value;
            }
        }

        /// <summary>
        ///   Indication of the load this query generates (e.g. the number of statements in the batch). Used by connection stratagies for balancing
        ///   queries over connections.
        /// </summary>
        /// <value> The load. Defaults to 1 </value>
        public int Load
        {
            get
            {
                CheckIfDisposed();
                return _batchCommand.Load;
            }
            set
            {
                CheckIfDisposed();
                _batchCommand.Load = value;
            }
        }

        /// <summary>
        ///   Gets or sets a value indicating whether tracing enabled should be enabled.
        /// </summary>
        /// <value> <c>true</c> if tracing enabled; otherwise, <c>false</c> . </value>
        public bool EnableTracing
        {
            get
            {
                CheckIfDisposed();
                return _batchCommand.EnableTracing;
            }
            set
            {
                CheckIfDisposed();
                _batchCommand.EnableTracing = value;
            }
        }

        /// <summary>
        ///   Gets the last batch query result. Contains a reference to any tracing identifier
        /// </summary>
        /// <value> The last query result. </value>
        public CqlVoid LastBatchResult
        {
            get
            {
                CheckIfDisposed();
                return (CqlVoid)_batchCommand.LastQueryResult;
            }
        }

        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_commands != null)
                {
                    _commands.Clear();
                    _commands = null;
                }

                if (_batchCommand != null)
                {
                    _batchCommand.Dispose();
                    _batchCommand = null;
                }
            }

            base.Dispose(disposing);
        }

        private void CheckIfDisposed()
        {
            if (_batchCommand == null)
                throw new ObjectDisposedException("CqlBatchTransaction");

        }

        /// <summary>
        ///   Cancels the execution of this batch.
        /// </summary>
        public void Cancel()
        {
            CheckIfDisposed();
            _batchCommand.Cancel();
        }

        /// <summary>
        ///   Commits the database transaction.
        /// </summary>
        /// <filterpriority>1</filterpriority>
        public override void Commit()
        {
            CheckIfDisposed();

            if (Connection.State == ConnectionState.Open)
            {
                if (_commands == null)
                    throw new InvalidOperationException("Transaction has been rolled back");

                if (_commands.Count > 0)
                    _batchCommand.ExecuteBatch();
            }
            else
            {
                Rollback();
                throw new InvalidOperationException("Commit error: Connection is closed or disposed");
            }
        }


        /// <summary>
        ///   Commits the database transaction asynchronously.
        /// </summary>
        /// <filterpriority>1</filterpriority>
        public Task CommitAsync()
        {
            return CommitAsync(CancellationToken.None);
        }

        /// <summary>
        ///   Commits the database transaction asynchronously.
        /// </summary>
        /// <param name="cancellationToken"> The cancellation token. </param>
        /// <returns> </returns>
        public Task CommitAsync(CancellationToken cancellationToken)
        {
            CheckIfDisposed();

            if (Connection.State == ConnectionState.Open)
            {

                if (_commands == null)
                    throw new InvalidOperationException("Transaction has been rolled back");

                if (_commands.Count > 0)
                    return _batchCommand.ExecuteBatchAsync(cancellationToken);

                return TaskCache.CompletedTask;
            }

            Rollback();
            throw new InvalidOperationException("Commit error: Connection is closed or disposed");
        }

        /// <summary>
        ///   Rolls back a transaction from a pending state.
        /// </summary>
        /// <filterpriority>1</filterpriority>
        public override void Rollback()
        {
            if (_commands != null)
            {
                _commands.Clear();
                _commands = null;
            }
        }
    }
}