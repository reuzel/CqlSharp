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

using CqlSharp.Linq.Mutations;
using CqlSharp.Linq.Query;
using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp.Linq
{
    /// <summary>
    ///   A representation of a Cql database (keyspace)
    /// </summary>
    public abstract class CqlContext : IDisposable
    {
        private readonly CqlDatabase _database;

        /// <summary>
        ///   The list of tables known to this context
        /// </summary>
        private readonly ConcurrentDictionary<Type, ICqlTable> _tables;

        private CqlChangeTracker _changeTracker;
        private bool _trackChanges;

        /// <summary>
        ///   Gets the CQL query provider.
        /// </summary>
        /// <value> The CQL query provider. </value>
        internal CqlQueryProvider QueryProvider { get; private set; }

        /// <summary>
        /// Gets or sets a value indicating whether this instance of the context will track changes to entities.
        /// </summary>
        /// <value>
        ///   <c>true</c> if [track changes]; otherwise, <c>false</c>.
        /// </value>
        public bool TrackChanges
        {
            get { return _trackChanges; }
            set
            {
                _trackChanges = value;

                if (!TrackChanges && _changeTracker != null)
                    _changeTracker.Clear();
            }
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlContext" /> class.
        /// </summary>
        /// <param name="initializeTables"> indicates wether the table properties are to be automatically initialized </param>
        protected CqlContext(bool initializeTables = true)
        {
            _database = new CqlDatabase(this);
            _tables = new ConcurrentDictionary<Type, ICqlTable>();

            QueryProvider = new CqlQueryProvider(this);

            TrackChanges = true;

            if (initializeTables)
                InitializeTables();
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlContext" /> class.
        /// </summary>
        /// <param name="connectionString"> The connection string. </param>
        /// <param name="initializeTables"> indicates wether the table properties are to be automatically initialized </param>
        protected CqlContext(string connectionString, bool initializeTables = true)
            : this(initializeTables)
        {
            _database.ConnectionString = connectionString;
        }

        protected CqlContext(CqlConnection connection, bool ownsConnection = true, bool initializeTables = true)
            : this(initializeTables)
        {
            _database.SetConnection(connection, ownsConnection);
        }

        /// <summary>
        ///   Gets the database underlying this context
        /// </summary>
        /// <value> The database. </value>
        public CqlDatabase Database
        {
            get { return _database; }
        }
        
        #region IDisposable Members

        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            _database.Dispose();
        }

        #endregion

        /// <summary>
        ///   Initializes the tables of this context.
        /// </summary>
        private void InitializeTables()
        {
            var properties = GetType().GetProperties();
            foreach (var property in properties)
            {
                var propertyType = property.PropertyType;
                if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(CqlTable<>))
                {
                    //create new table object
                    var table =
                        (ICqlTable)
                        Activator.CreateInstance(propertyType, BindingFlags.Public | BindingFlags.Instance, null,
                                                 new object[] { this }, null);

                    //add it to the list of known tables
                    table = _tables.GetOrAdd(table.EntityType, table);

                    //set the property
                    property.SetValue(this, table);
                }
            }
        }

        /// <summary>
        ///   Gets the table represented by the provided entityType.
        /// </summary>
        /// <typeparam name="TEntity"> type that represents the values in the table </typeparam>
        /// <returns> a CqlTable </returns>
        public CqlTable<TEntity> GetTable<TEntity>() where TEntity : class, new()
        {
            return (CqlTable<TEntity>)_tables.GetOrAdd(typeof(TEntity), new CqlTable<TEntity>(this));
        }

        /// <summary>
        ///   Gets the mutation tracker.
        /// </summary>
        /// <value> The mutation tracker that keeps track of changes to all entities</value>
        public CqlChangeTracker ChangeTracker
        {
            get
            {
                if (_changeTracker == null)
                {
                    var tracker = new CqlChangeTracker(this);
                    Interlocked.CompareExchange(ref _changeTracker, tracker, null);
                }

                return _changeTracker;
            }
        }

        #region SaveChanges

        /// <summary>
        /// Saves the changes.
        /// </summary>
        public void SaveChanges()
        {
            if (TrackChanges)
                ChangeTracker.SaveChanges(CqlConsistency.One, true);
        }

        /// <summary>
        /// Saves the changes with the required consistency level.
        /// </summary>
        /// <param name="acceptChangesDuringSave">if set to <c>true</c> [accept changes during save].</param>
        public void SaveChanges(bool acceptChangesDuringSave)
        {
            if (TrackChanges)
                ChangeTracker.SaveChanges(CqlConsistency.One, acceptChangesDuringSave);
        }


        /// <summary>
        ///   Saves the changes with the required consistency level.
        /// </summary>
        /// <param name="consistency"> The consistency level. Defaults to one. </param>
        public void SaveChanges(CqlConsistency consistency)
        {
            if (TrackChanges)
                ChangeTracker.SaveChanges(consistency, true);
        }


        /// <summary>
        /// Saves the changes with the required consistency level.
        /// </summary>
        /// <param name="consistency">The consistency level. Defaults to one.</param>
        /// <param name="acceptChangesDuringSave">if set to <c>true</c> [accept changes during save].</param>
        public void SaveChanges(CqlConsistency consistency, bool acceptChangesDuringSave)
        {
            if (TrackChanges)
                ChangeTracker.SaveChanges(consistency, acceptChangesDuringSave);
        }


        /// <summary>
        ///   Saves the changes
        /// </summary>
        public Task SaveChangesAsync()
        {
            return SaveChangesAsync(CqlConsistency.One, true, CancellationToken.None);
        }

        /// <summary>
        ///   Saves the changes.
        /// </summary>
        /// <param name="cancellationToken"> the cancellation token </param>
        public Task SaveChangesAsync(CancellationToken cancellationToken)
        {
            return SaveChangesAsync(CqlConsistency.One, true, cancellationToken);
        }

        /// <summary>
        ///   Saves the changes with the required consistency level.
        /// </summary>
        /// <param name="consistency"> The consistency level. Defaults to one. </param>
        public Task SaveChangesAsync(CqlConsistency consistency)
        {
            return SaveChangesAsync(consistency, true, CancellationToken.None);
        }

        /// <summary>
        /// Saves the changes with the required consistency level.
        /// </summary>
        /// <param name="consistency">The consistency level. Defaults to one.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns></returns>
        public Task SaveChangesAsync(CqlConsistency consistency, CancellationToken cancellationToken)
        {
            return SaveChangesAsync(consistency, true, cancellationToken);
        }

        /// <summary>
        /// Saves the changes with the required consistency level.
        /// </summary>
        /// <param name="consistency">The consistency level</param>
        /// <param name="acceptChangesDuringSave">if set to <c>true</c> [accept changes during save].</param>
        /// <returns></returns>
        public Task SaveChangesAsync(CqlConsistency consistency, bool acceptChangesDuringSave)
        {
            return SaveChangesAsync(consistency, acceptChangesDuringSave, CancellationToken.None);
        }

        /// <summary>
        /// Saves the changes with the required consistency level.
        /// </summary>
        /// <param name="consistency">The consistency level</param>
        /// <param name="acceptChangesDuringSave">if set to <c>true</c> [accept changes during save].</param>
        /// <param name="cancellationToken">the cancellation token</param>
        /// <returns></returns>
        public Task SaveChangesAsync(CqlConsistency consistency, bool acceptChangesDuringSave,
                                     CancellationToken cancellationToken)
        {
            if (TrackChanges)
                return ChangeTracker.SaveChangesAsync(consistency, acceptChangesDuringSave, cancellationToken);

            return Task.FromResult(false);
        }


        #endregion

        /// <summary>
        /// Accepts all changes made to tracked entities, and regards them as unchanged afterwards.
        /// </summary>
        public void AcceptAllChanges()
        {
            if (TrackChanges)
                ChangeTracker.AcceptAllChanges();
        }
    }
}