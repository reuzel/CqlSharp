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

using System.Data;
using System.Data.Common;

namespace CqlSharp
{
    /// <summary>
    /// </summary>
    internal class CqlDataAdapter : DbDataAdapter, IDbDataAdapter
    {
        /*
         * Inherit from Component through DbDataAdapter. The event
         * mechanism is designed to work with the Component.Events
         * property. These variables are the keys used to find the
         * events in the components list of events.
         */
        private static readonly object EventRowUpdated = new object();
        private static readonly object EventRowUpdating = new object();
        private CqlCommand _deleteCommand;
        private CqlCommand _insertCommand;
        private CqlCommand _selectCommand;
        private CqlCommand _updateCommand;

        /// <summary>
        ///   Gets or sets a command used to select records in the data source.
        /// </summary>
        /// <returns> A <see cref="T:System.Data.IDbCommand" /> that is used during <see
        ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> to select records from data source for placement in the data set. </returns>
        public new CqlCommand SelectCommand
        {
            get { return _selectCommand; }
            set { _selectCommand = value; }
        }

        /// <summary>
        ///   Gets or sets a command used to insert new records into the data source.
        /// </summary>
        /// <returns> A <see cref="T:System.Data.IDbCommand" /> used during <see
        ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> to insert records in the data source for new rows in the data set. </returns>
        public new CqlCommand InsertCommand
        {
            get { return _insertCommand; }
            set { _insertCommand = value; }
        }

        /// <summary>
        ///   Gets or sets a command used to update records in the data source.
        /// </summary>
        /// <returns> A <see cref="T:System.Data.IDbCommand" /> used during <see
        ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> to update records in the data source for modified rows in the data set. </returns>
        public new CqlCommand UpdateCommand
        {
            get { return _updateCommand; }
            set { _updateCommand = value; }
        }

        /// <summary>
        ///   Gets or sets a command for deleting records from the data set.
        /// </summary>
        /// <returns> An <see cref="T:System.Data.IDbCommand" /> used during <see
        ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> to delete records in the data source for deleted rows in the data set. </returns>
        public new CqlCommand DeleteCommand
        {
            get { return _deleteCommand; }
            set { _deleteCommand = value; }
        }

        #region IDbDataAdapter Members

        /// <summary>
        ///   Gets or sets a command used to select records in the data source.
        /// </summary>
        /// <returns> A <see cref="T:System.Data.IDbCommand" /> that is used during <see
        ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> to select records from data source for placement in the data set. </returns>
        IDbCommand IDbDataAdapter.SelectCommand
        {
            get { return _selectCommand; }
            set { _selectCommand = (CqlCommand)value; }
        }

        /// <summary>
        ///   Gets or sets a command used to insert new records into the data source.
        /// </summary>
        /// <returns> A <see cref="T:System.Data.IDbCommand" /> used during <see
        ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> to insert records in the data source for new rows in the data set. </returns>
        IDbCommand IDbDataAdapter.InsertCommand
        {
            get { return _insertCommand; }
            set { _insertCommand = (CqlCommand)value; }
        }

        /// <summary>
        ///   Gets or sets a command used to update records in the data source.
        /// </summary>
        /// <returns> A <see cref="T:System.Data.IDbCommand" /> used during <see
        ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> to update records in the data source for modified rows in the data set. </returns>
        IDbCommand IDbDataAdapter.UpdateCommand
        {
            get { return _updateCommand; }
            set { _updateCommand = (CqlCommand)value; }
        }

        /// <summary>
        ///   Gets or sets a command for deleting records from the data set.
        /// </summary>
        /// <returns> An <see cref="T:System.Data.IDbCommand" /> used during <see
        ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> to delete records in the data source for deleted rows in the data set. </returns>
        IDbCommand IDbDataAdapter.DeleteCommand
        {
            get { return _deleteCommand; }
            set { _deleteCommand = (CqlCommand)value; }
        }

        #endregion


        /*
         * Implement abstract methods inherited from DbDataAdapter.
         */

        /// <summary>
        ///   Initializes a new instance of the <see cref="T:System.Data.Common.RowUpdatedEventArgs" /> class.
        /// </summary>
        /// <param name="dataRow"> The <see cref="T:System.Data.DataRow" /> used to update the data source. </param>
        /// <param name="command"> The <see cref="T:System.Data.IDbCommand" /> executed during the <see
        ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> . </param>
        /// <param name="statementType"> Whether the command is an UPDATE, INSERT, DELETE, or SELECT statement. </param>
        /// <param name="tableMapping"> A <see cref="T:System.Data.Common.DataTableMapping" /> object. </param>
        /// <returns> A new instance of the <see cref="T:System.Data.Common.RowUpdatedEventArgs" /> class. </returns>
        protected override RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, IDbCommand command,
                                                                     StatementType statementType,
                                                                     DataTableMapping tableMapping)
        {
            return new CqlRowUpdatedEventArgs(dataRow, command, statementType, tableMapping);
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="T:System.Data.Common.RowUpdatingEventArgs" /> class.
        /// </summary>
        /// <param name="dataRow"> The <see cref="T:System.Data.DataRow" /> that updates the data source. </param>
        /// <param name="command"> The <see cref="T:System.Data.IDbCommand" /> to execute during the <see
        ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> . </param>
        /// <param name="statementType"> Whether the command is an UPDATE, INSERT, DELETE, or SELECT statement. </param>
        /// <param name="tableMapping"> A <see cref="T:System.Data.Common.DataTableMapping" /> object. </param>
        /// <returns> A new instance of the <see cref="T:System.Data.Common.RowUpdatingEventArgs" /> class. </returns>
        protected override RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, IDbCommand command,
                                                                       StatementType statementType,
                                                                       DataTableMapping tableMapping)
        {
            return new CqlRowUpdatingEventArgs(dataRow, command, statementType, tableMapping);
        }

        /// <summary>
        ///   Raises the RowUpdating event of a .NET Framework data provider.
        /// </summary>
        /// <param name="value"> An <see cref="T:System.Data.Common.RowUpdatingEventArgs" /> that contains the event data. </param>
        protected override void OnRowUpdating(RowUpdatingEventArgs value)
        {
            var handler = (CqlRowUpdatingEventHandler)Events[EventRowUpdating];
            if ((null != handler) && (value is CqlRowUpdatingEventArgs))
            {
                handler(this, (CqlRowUpdatingEventArgs)value);
            }
        }

        /// <summary>
        ///   Raises the RowUpdated event of a .NET Framework data provider.
        /// </summary>
        /// <param name="value"> A <see cref="T:System.Data.Common.RowUpdatedEventArgs" /> that contains the event data. </param>
        protected override void OnRowUpdated(RowUpdatedEventArgs value)
        {
            var handler = (CqlRowUpdatedEventHandler)Events[EventRowUpdated];
            if ((null != handler) && (value is CqlRowUpdatedEventArgs))
            {
                handler(this, (CqlRowUpdatedEventArgs)value);
            }
        }

        /// <summary>
        ///   Occurs when [row updating].
        /// </summary>
        public event CqlRowUpdatingEventHandler RowUpdating
        {
            add { Events.AddHandler(EventRowUpdating, value); }
            remove { Events.RemoveHandler(EventRowUpdating, value); }
        }

        /// <summary>
        ///   Occurs when [row updated].
        /// </summary>
        public event CqlRowUpdatedEventHandler RowUpdated
        {
            add { Events.AddHandler(EventRowUpdated, value); }
            remove { Events.RemoveHandler(EventRowUpdated, value); }
        }
    }

    public delegate void CqlRowUpdatingEventHandler(object sender, CqlRowUpdatingEventArgs e);

    public delegate void CqlRowUpdatedEventHandler(object sender, CqlRowUpdatedEventArgs e);

    /// <summary>
    /// </summary>
    public class CqlRowUpdatingEventArgs : RowUpdatingEventArgs
    {
        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlRowUpdatingEventArgs" /> class.
        /// </summary>
        /// <param name="row"> The row. </param>
        /// <param name="command"> The command. </param>
        /// <param name="statementType"> Type of the statement. </param>
        /// <param name="tableMapping"> The table mapping. </param>
        public CqlRowUpdatingEventArgs(DataRow row, IDbCommand command, StatementType statementType,
                                       DataTableMapping tableMapping)
            : base(row, command, statementType, tableMapping)
        {
        }

        /// <summary>
        ///   Gets the <see cref="T:System.Data.IDbCommand" /> to execute during the <see
        ///    cref="M:System.Data.Common.DbDataAdapter.Update(System.Data.DataSet)" /> operation.
        /// </summary>
        /// <returns> The <see cref="T:System.Data.IDbCommand" /> to execute during the <see
        ///    cref="M:System.Data.Common.DbDataAdapter.Update(System.Data.DataSet)" /> . </returns>
        public new CqlCommand Command
        {
            get { return (CqlCommand)base.Command; }
            set { base.Command = value; }
        }
    }

    /// <summary>
    /// </summary>
    public class CqlRowUpdatedEventArgs : RowUpdatedEventArgs
    {
        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlRowUpdatedEventArgs" /> class.
        /// </summary>
        /// <param name="row"> The row. </param>
        /// <param name="command"> The command. </param>
        /// <param name="statementType"> Type of the statement. </param>
        /// <param name="tableMapping"> The table mapping. </param>
        public CqlRowUpdatedEventArgs(DataRow row, IDbCommand command, StatementType statementType,
                                      DataTableMapping tableMapping)
            : base(row, command, statementType, tableMapping)
        {
        }

        /// <summary>
        ///   Gets the <see cref="T:System.Data.IDbCommand" /> executed when <see
        ///    cref="M:System.Data.Common.DbDataAdapter.Update(System.Data.DataSet)" /> is called.
        /// </summary>
        /// <returns> The <see cref="T:System.Data.IDbCommand" /> executed when <see
        ///    cref="M:System.Data.Common.DbDataAdapter.Update(System.Data.DataSet)" /> is called. </returns>
        public new CqlCommand Command
        {
            get { return (CqlCommand)base.Command; }
        }
    }
}