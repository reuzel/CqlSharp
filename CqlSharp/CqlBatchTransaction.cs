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
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using CqlSharp.Protocol;

namespace CqlSharp
{
    /// <summary>
    /// A batched query. Any commands executed as part of a <see cref="CqlBatchTransaction" /> will be
    /// buffered and executed in a single batch when committed.
    /// </summary>
    public class CqlBatchTransaction : DbTransaction
    {
        /// <summary>
        /// the possible states this transaction is in.
        /// </summary>
        private enum TransactionState
        {
            Pending,
            Committed,
            RolledBack,
            Disposed
        }

        /// <summary>
        /// The command representing the batch operation
        /// </summary>
        private readonly CqlCommand _batchCommand;

        /// <summary>
        /// the series of commands enlisted with this transaction
        /// </summary>
        private readonly List<BatchFrame.BatchedCommand> _commands;

        /// <summary>
        /// the state of the transaction
        /// </summary>
        private TransactionState _state;

        /// <summary>
        /// The batch type
        /// </summary>
        private CqlBatchType _batchType;

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlBatchTransaction" /> class.
        /// </summary>
        public CqlBatchTransaction()
        {
            _batchCommand = new CqlCommand {Transaction = this};
            _commands = new List<BatchFrame.BatchedCommand>();
            _batchType = CqlBatchType.Logged;
            _state = TransactionState.Pending;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlBatchTransaction" /> class.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        public CqlBatchTransaction(CqlConnection connection)
            : this(connection, CqlBatchType.Logged, CqlConsistency.One)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlBatchTransaction" /> class.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="batchType"> Type of the batch. </param>
        public CqlBatchTransaction(CqlConnection connection, CqlBatchType batchType)
            : this(connection, batchType, CqlConsistency.One)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlBatchTransaction" /> class.
        /// </summary>
        /// <param name="connection"> The connection. </param>
        /// <param name="batchType"> Type of the batch. </param>
        /// <param name="consistency"> The consistency. </param>
        public CqlBatchTransaction(CqlConnection connection, CqlBatchType batchType, CqlConsistency consistency)
        {
            _batchCommand = new CqlCommand(connection) {Consistency = consistency, Transaction = this};
            _commands = new List<BatchFrame.BatchedCommand>();
            _batchType = batchType;
            _state = TransactionState.Pending;
        }

        /// <summary>
        /// Gets or sets the wait time before terminating the attempt to execute a (batch) command and generating an error.
        /// </summary>
        /// <returns> The time (in seconds) to wait for the command to execute. The default value is 30 seconds. </returns>
        public virtual int CommandTimeout
        {
            get { return _batchCommand.CommandTimeout; }
            set { _batchCommand.CommandTimeout = value; }
            }

        /// <summary>
        /// Specifies the <see cref="T:System.Data.Common.DbConnection" /> object associated with the transaction.
        /// </summary>
        /// <returns> The <see cref="T:System.Data.Common.DbConnection" /> object associated with the transaction. </returns>
        protected override DbConnection DbConnection
        {
            get { return Connection; }
        }

        /// <summary>
        /// Specifies the <see cref="T:CqlSharp.CqlConnection" /> object associated with the transaction.
        /// </summary>
        /// <returns> The <see cref="T:CqlSharp.CqlConnection" /> object associated with the transaction. </returns>
        public new virtual CqlConnection Connection
        {
            get { return _batchCommand.Connection; }
            set { _batchCommand.Connection = value; }
            }

        /// <summary>
        /// Specifies the <see cref="T:System.Data.IsolationLevel" /> for this transaction.
        /// </summary>
        /// <returns> The <see cref="T:System.Data.IsolationLevel" /> for this transaction. </returns>
        /// <filterpriority>1</filterpriority>
        public override IsolationLevel IsolationLevel
        {
            get { return IsolationLevel.Unspecified; }
        }

        /// <summary>
        /// Gets or sets the type of the batch.
        /// </summary>
        /// <value> The type of the batch. </value>
        public virtual CqlBatchType BatchType
        {
            get { return _batchType; }
            set { _batchType = value; }
        }

        /// <summary>
        /// Gets the commands.
        /// </summary>
        /// <value> The commands. </value>
        internal List<BatchFrame.BatchedCommand> Commands
        {
            get
            {
                CheckIfPending();
                return _commands;
            }
        }

        /// <summary>
        /// Gets or sets the consistency.
        /// </summary>
        /// <value> The consistency. </value>
        public virtual CqlConsistency Consistency
        {
            get { return _batchCommand.Consistency; }
            set { _batchCommand.Consistency = value; }
            }

        /// <summary>
        /// Indication of the load this query generates (e.g. the number of statements in the batch). Used by connection stratagies
        /// for balancing
        /// queries over connections.
        /// </summary>
        /// <value> The load. Defaults to 1 </value>
        public virtual int Load
        {
            get { return _batchCommand.Load; }
            set { _batchCommand.Load = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether tracing enabled should be enabled.
        /// </summary>
        /// <value> <c>true</c> if tracing enabled; otherwise, <c>false</c> . </value>
        public virtual bool EnableTracing
        {
            get { return _batchCommand.EnableTracing; }
            set { _batchCommand.EnableTracing = value; }
        }

        /// <summary>
        /// Gets the last batch query result. Contains a reference to any tracing identifier
        /// </summary>
        /// <value> The last query result. </value>
        public virtual ICqlQueryResult LastBatchResult
        {
            get { return _batchCommand.LastQueryResult; }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        protected override void Dispose(bool disposing)
        {
            if(disposing)
            {
                _state = TransactionState.Disposed;
                _commands.Clear();
            }

            base.Dispose(disposing);
        }

        /// <summary>
        /// Checks wether this transaction is in the pending state
        /// </summary>
        /// <exception cref="System.ObjectDisposedException">CqlBatchTransaction</exception>
        /// <exception cref="System.InvalidOperationException">
        /// Transaction has already been committed
        /// or
        /// Transaction has been rolled back
        /// </exception>
        private void CheckIfPending()
        {
            if(_state == TransactionState.Disposed)
                throw new ObjectDisposedException("CqlBatchTransaction");

            if(_state == TransactionState.Committed)
                throw new InvalidOperationException("Transaction has already been committed");

            if(_state == TransactionState.RolledBack)
                throw new InvalidOperationException("Transaction has been rolled back");
        }

        /// <summary>
        /// Cancels the execution of this batch.
        /// </summary>
        public virtual void Cancel()
        {
            _batchCommand.Cancel();
        }

        /// <summary>
        /// Commits the database transaction.
        /// </summary>
        /// <filterpriority>1</filterpriority>
        public override void Commit()
        {
            CheckIfPending();

            if(Connection.State == ConnectionState.Open)
            {
                if(_commands.Count > 0)
                    _batchCommand.ExecuteBatch();

                _state = TransactionState.Committed;
            }
            else
                throw new InvalidOperationException("Commit error: Connection is closed or disposed");
            }


        /// <summary>
        /// Commits the database transaction asynchronously.
        /// </summary>
        /// <filterpriority>1</filterpriority>
        public virtual Task CommitAsync()
        {
            return CommitAsync(CancellationToken.None);
        }


        /// <summary>
        /// Commits the database transaction asynchronously.
        /// </summary>
        /// <param name="cancellationToken"> The cancellation token. </param>
        /// <returns> </returns>
        public virtual Task CommitAsync(CancellationToken cancellationToken)
        {
            CheckIfPending();

            if(Connection.State == ConnectionState.Open)
                return CommitAsyncInternal(cancellationToken);

            throw new InvalidOperationException("Commit error: Connection is closed or disposed");
        }

        /// <summary>
        /// Performs the actual asynchronous commit operation
        /// </summary>
        /// <param name="cancellationToken"> The cancellation token. </param>
        /// <returns> </returns>
        private async Task CommitAsyncInternal(CancellationToken cancellationToken)
        {
            if (_commands.Count > 0)
                await _batchCommand.ExecuteBatchAsync(cancellationToken).ConfigureAwait(false);
            
            _state = TransactionState.Committed;
        }

        /// <summary>
        /// Rolls back a transaction from a pending state.
        /// </summary>
        /// <filterpriority>1</filterpriority>
        public override void Rollback()
        {
            CheckIfPending();

            _state = TransactionState.RolledBack;
            _commands.Clear();
        }

        /// <summary>
        /// Resets this transaction. This clears the list of commands that are part of the transaction, and brings
        /// the transaction in the same state as if it was newly created
        /// </summary>
        public virtual void Reset()
        {
            _state = TransactionState.Pending;
            _commands.Clear();
        }
    }
}