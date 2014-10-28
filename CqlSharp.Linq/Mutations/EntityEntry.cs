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
using System.Collections;
using System.Collections.Generic;
using System.Data;
using CqlSharp.Serialization;

namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    ///   Tracks the changes to a single object of a specific type
    /// </summary>
    /// <typeparam name="TEntity"> The type of the entity. </typeparam>
    public class EntityEntry<TEntity> : IEntityEntry where TEntity : class, new()
    {
        private TEntity _original;

        /// <summary>
        ///   Initializes a new instance of the <see cref="EntityEntry{TEntity}" /> class.
        /// </summary>
        /// <param name="table"> The table. </param>
        /// <param name="key"> the key of the entity being tracked </param>
        /// <param name="entity"> The entity. </param>
        /// <param name="original"> The original. </param>
        /// <param name="entityState"> State of the object. </param>
        public EntityEntry(CqlTable<TEntity> table, EntityKey<TEntity> key, TEntity entity, TEntity original,
                             EntityState entityState)
        {
            Original = original;
            Table = table;
            Key = key;
            Entity = entity;
            State = entityState;
        }

        /// <summary>
        ///   Gets the table this entity belongs to
        /// </summary>
        /// <value> The table. </value>
        public CqlTable<TEntity> Table { get; private set; }

        /// <summary>
        ///   Gets the changed columns.
        /// </summary>
        /// <value> The changed columns. </value>
        internal List<ICqlColumnInfo<TEntity>> ChangedColumns { get; private set; }

        /// <summary>
        ///   Gets the key defining this entity.
        /// </summary>
        /// <value> The key. </value>
        public EntityKey<TEntity> Key { get; private set; }

        /// <summary>
        ///   Gets the entity being tracked.
        /// </summary>
        /// <value> The entity. </value>
        public TEntity Entity { get; private set; }

        /// <summary>
        ///   Gets the state of this entity (added/removed/unchanged).
        /// </summary>
        /// <value> The state. </value>
        public EntityState State { get; internal set; }

        /// <summary>
        ///   Gets the original values of the object
        /// </summary>
        /// <value> The object. </value>
        public TEntity Original
        {
            get { return EntityHelper<TEntity>.Instance.Clone(_original); }
            private set { _original = value; }
        }

        #region IEntityEntry Members

        /// <summary>
        ///   Gets the table this entity is part of.
        /// </summary>
        /// <value> The table. </value>
        ICqlTable IEntityEntry.Table
        {
            get { return Table; }
        }

        /// <summary>
        ///   Gets the key that uniquely identifies this entity within the table.
        /// </summary>
        /// <value> The key. </value>
        IEntityKey IEntityEntry.Key
        {
            get { return Key; }
        }

        /// <summary>
        ///   Gets the current entity values
        /// </summary>
        /// <value> The entity. </value>
        object IEntityEntry.Entity
        {
            get { return Entity; }
        }

        /// <summary>
        ///   Gets the original values as last read from the database.
        /// </summary>
        /// <value> The original. </value>
        object IEntityEntry.Original
        {
            get { return Original; }
        }

        /// <summary>
        ///   Gets the modification state of this entity .
        /// </summary>
        /// <value> The state. </value>
        EntityState IEntityEntry.State
        {
            get { return State; }
        }

        /// <summary>
        ///   Sets the original values of this entity.
        /// </summary>
        /// <param name="newOriginal"> The new original. </param>
        void IEntityEntry.SetOriginalValues(object newOriginal)
        {
            SetOriginalValues((TEntity) newOriginal);
        }

        /// <summary>
        ///   Sets the object values.
        /// </summary>
        /// <param name="newValues"> The new values. </param>
        void IEntityEntry.SetEntityValues(object newValues)
        {
            SetEntityValues((TEntity) newValues);
        }

        /// <summary>
        ///   Reloads this instance from the database, effectively making it unchanged.
        /// </summary>
        void IEntityEntry.Reload()
        {
            Reload();
        }

        #endregion

        /// <summary>
        ///   Detects the changes.
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">Illegal change detected: A tracked object has changed its key</exception>
        internal bool DetectChanges()
        {
            if (State == EntityState.Detached)
                return false;

            if (State == EntityState.Deleted)
                return true;

            //make sure the entity did not switch key
            if (!Key.IsKeyOf(Entity))
                throw new CqlLinqException("Illegal change detected: A tracked entity has changed its key");

            if (State == EntityState.Added)
                return true;

            //find which columns have changed
            var changedColumns = new List<ICqlColumnInfo<TEntity>>();
            foreach (ICqlColumnInfo<TEntity> column in ObjectAccessor<TEntity>.Instance.NormalColumns)
            {
                var original = column.Read<object>(Original);
                var actual = column.Read<object>(Entity);
                
                if (column.CqlType.CqlTypeCode == CqlTypeCode.List || column.CqlType.CqlTypeCode == CqlTypeCode.Map || column.CqlType.CqlTypeCode == CqlTypeCode.Set)
                {
                    if (!TypeSystem.SequenceEqual((IEnumerable) original, (IEnumerable) actual))
                        changedColumns.Add(column);
                }
                else if (!Equals(original, actual))
                {
                    changedColumns.Add(column);
                }
            }

            //update property
            ChangedColumns = changedColumns;

            //update state
            if (changedColumns.Count > 0)
            {
                State = EntityState.Modified;
                return true;
            }

            State = EntityState.Unchanged;
            return false;
        }

        /// <summary>
        ///   Gets the DML statement.
        /// </summary>
        /// <returns> </returns>
        internal string GetDmlStatement()
        {
            return CqlBuilder<TEntity>.BuildDmlQuery(this);
        }

        /// <summary>
        ///   Sets the original values
        /// </summary>
        /// <param name="newOriginal"> The values to use as original values, which should represent the known database state </param>
        public void SetOriginalValues(TEntity newOriginal)
        {
            if (!Key.IsKeyOf(newOriginal))
                throw new ArgumentException(
                    "The new original values represent an different entity than the one tracked. The key values do not match",
                    "newOriginal");

            Original = EntityHelper<TEntity>.Instance.Clone(newOriginal);
        }

        /// <summary>
        ///   Sets object values
        /// </summary>
        /// <param name="newValues"> The values to use as object values, which should represent the new (uncommitted) database state </param>
        public void SetEntityValues(TEntity newValues)
        {
            if (!Key.IsKeyOf(newValues))
                throw new ArgumentException(
                    "The new object values represent an different entity than the one tracked. The key values do not match",
                    "newValues");

            EntityHelper<TEntity>.Instance.CopyTo(newValues, Entity);
        }

        /// <summary>
        ///   Reloads this instance.
        /// </summary>
        public void Reload()
        {
            var connection = Table.Context.Database.Connection;
            if (connection.State == ConnectionState.Closed)
                connection.Open();

            var cql = CqlBuilder<TEntity>.GetSelectQuery(Table, Key);
            Table.Context.Database.LogQuery(cql);

            var command = new CqlCommand(connection, cql);

            using (var reader = command.ExecuteReader<TEntity>())
            {
                if (reader.Read())
                {
                    var row = reader.Current;
                    SetOriginalValues(row);
                    SetEntityValues(row);
                    State = EntityState.Unchanged;
                }
                else
                {
                    State = EntityState.Detached;
                }
            }
        }
    }
}