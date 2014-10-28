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

using System.Collections.Generic;
using System.Linq;

namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    ///   Tracks changes for a specific table
    /// </summary>
    /// <typeparam name="TEntity"> The type of the entity. </typeparam>
    internal class TableChangeTracker<TEntity> : ITableChangeTracker where TEntity : class, new()
    {
        private readonly object _syncLock = new object();

        /// <summary>
        ///   the table for which changes are tracked
        /// </summary>
        private readonly CqlTable<TEntity> _table;


        private readonly Dictionary<TEntity, EntityEntry<TEntity>> _trackedEntities;

        /// <summary>
        ///   The tracked objects stored by reference and by key
        /// </summary>
        private readonly Dictionary<EntityKey<TEntity>, EntityEntry<TEntity>> _trackedEntitiesByKey;


        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlChangeTracker" /> class.
        /// </summary>
        public TableChangeTracker(CqlTable<TEntity> table)
        {
            _table = table;
            _trackedEntities =
                new Dictionary<TEntity, EntityEntry<TEntity>>(ObjectReferenceEqualityComparer<TEntity>.Instance);
            _trackedEntitiesByKey = new Dictionary<EntityKey<TEntity>, EntityEntry<TEntity>>();
        }

        #region ITableChangeTracker Members

        /// <summary>
        ///   Gets a value indicating whether this table has any changes.
        /// </summary>
        /// <value> <c>true</c> if [has changes]; otherwise, <c>false</c> . </value>
        public bool HasChanges()
        {
            lock (_syncLock)
            {
                return _trackedEntities.Values.Any(te => te.State != EntityState.Unchanged);
            }
        }

        /// <summary>
        ///   Detects all changes in the tracked entities.
        /// </summary>
        /// <returns> </returns>
        public bool DetectChanges()
        {
            bool hasChanges = false;
            // ReSharper disable LoopCanBeConvertedToQuery
            foreach (var trackedObject in Entries())
            {
                hasChanges |= trackedObject.DetectChanges();
            }
            // ReSharper restore LoopCanBeConvertedToQuery

            return hasChanges;
        }

        /// <summary>
        ///   enlists the changes to a transaction
        /// </summary>
        /// <param name="transaction"> transaction the changes is enlisted on </param>
        /// <param name="consistency"> The consistency. </param>
        /// <param name="connection"> connection to execute command on </param>
        public void EnlistChanges(CqlConnection connection, CqlBatchTransaction transaction, CqlConsistency consistency)
        {
            foreach (var trackedObject in _trackedEntities.Values)
            {
                if (trackedObject.State != EntityState.Unchanged)
                {
                    var cql = trackedObject.GetDmlStatement();
                    _table.Context.Database.LogQuery(cql);

                    var command = new CqlCommand(connection, cql, consistency) { Transaction = transaction };
                    command.ExecuteNonQuery();
                }
            }
        }

        public void AcceptAllChanges()
        {
            foreach (var trackedObject in _trackedEntities.Values)
            {
                switch (trackedObject.State)
                {
                    case EntityState.Deleted:
                        trackedObject.State = EntityState.Detached;
                        break;
                    case EntityState.Added:
                    case EntityState.Modified:
                        trackedObject.SetOriginalValues(trackedObject.Entity);
                        trackedObject.State = EntityState.Unchanged;
                        break;
                }
            }
        }

        IEnumerable<IEntityEntry> ITableChangeTracker.Entries()
        {
            return Entries();
        }

        #endregion

        /// <summary>
        ///   Adds the entity in an Added state.
        /// </summary>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        public bool Add(TEntity entity)
        {
            lock (_syncLock)
            {
                //check if entity already tracked
                if (_trackedEntities.ContainsKey(entity))
                    return false;

                //create a new key from object
                var key = EntityKey<TEntity>.Create(entity);

                //check if key already tracked
                if (_trackedEntitiesByKey.ContainsKey(key))
                    return false;

                //create new entry
                var entry = new EntityEntry<TEntity>(_table, key, entity, default(TEntity), EntityState.Added);

                //add the entry
                _trackedEntities.Add(entity, entry);
                _trackedEntitiesByKey.Add(key, entry);

                //okidoki
                return true;
            }
        }

        /// <summary>
        ///   Attaches the specified entity.
        /// </summary>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        public bool Attach(TEntity entity)
        {
            lock (_syncLock)
            {
                //check if entity already tracked
                if (_trackedEntities.ContainsKey(entity))
                    return false;

                //create a new key from object
                var key = EntityKey<TEntity>.Create(entity);

                //check if key already tracked
                if (_trackedEntitiesByKey.ContainsKey(key))
                    return false;

                //create new entry
                var original = EntityHelper<TEntity>.Instance.Clone(entity);
                var entry = new EntityEntry<TEntity>(_table, key, entity, original, EntityState.Unchanged);

                //add the entry
                _trackedEntities.Add(entity, entry);
                _trackedEntitiesByKey.Add(key, entry);

                //okidoki
                return true;
            }
        }

        /// <summary>
        ///   Detaches the specified entity.
        /// </summary>
        /// <returns> </returns>
        public bool Detach(TEntity entity)
        {
            lock (_syncLock)
            {
                EntityEntry<TEntity> entry;
                if (!_trackedEntities.TryGetValue(entity, out entry))
                    return false;

                //entry found, remove it
                _trackedEntities.Remove(entity);
                _trackedEntitiesByKey.Remove(entry.Key);

                //okidoki
                return true;
            }
        }

        /// <summary>
        ///   Deletes the specified entity.
        /// </summary>
        /// <param name="entity"> The entity. </param>
        public void Delete(TEntity entity)
        {
            lock (_syncLock)
            {
                EntityEntry<TEntity> entry;
                if (!_trackedEntities.TryGetValue(entity, out entry))
                {
                    //entity not tracked yet. Create entry.
                    var key = EntityKey<TEntity>.Create(entity);
                    entry = new EntityEntry<TEntity>(_table, key, entity,
                                                       default(TEntity), EntityState.Deleted);

                    //add the entry
                    _trackedEntities.Add(entity, entry);
                    _trackedEntitiesByKey.Add(key, entry);
                }

                entry.State = EntityState.Deleted;
            }
        }

        /// <summary>
        ///   Gets or adds the row as described by the entity. If an entity with an identical key
        ///   already is tracked, this already tracked entity is returned
        /// </summary>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        public TEntity GetOrAttach(TEntity entity)
        {
            lock (_syncLock)
            {
                //check if entity contained in entity reference list
                if (_trackedEntities.ContainsKey(entity))
                    return entity;

                //check if entity with same key is already tracked
                EntityEntry<TEntity> entry;
                if (_trackedEntitiesByKey.TryGetValue(new EntityKey<TEntity>(entity), out entry))
                {
                    return entry.Entity;
                }

                //entry not existing yet, attach it.
                Attach(entity);

                //return entity
                return entity;
            }
        }

        /// <summary>
        ///   Tries the get entity by key.
        /// </summary>
        /// <param name="key"> The key. </param>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        public bool TryGetEntityByKey(EntityKey<TEntity> key, out TEntity entity)
        {
            lock (_syncLock)
            {
                EntityEntry<TEntity> entry;
                if (_trackedEntitiesByKey.TryGetValue(key, out entry))
                {
                    entity = entry.Entity;
                    return true;
                }

                entity = default(TEntity);
                return false;
            }
        }

        public IEnumerable<TEntity> Entities()
        {
            return _trackedEntities.Keys;
        }

        public IEnumerable<EntityEntry<TEntity>> Entries()
        {
            return _trackedEntities.Values;
        }

        /// <summary>
        /// Tries to get the tracked entry related to the provided entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <param name="entry">The entry.</param>
        /// <returns></returns>
        public bool TryGetEntry(TEntity entity, out EntityEntry<TEntity> entry)
        {
            lock (_syncLock)
            {
                return _trackedEntities.TryGetValue(entity, out entry);
            }
        }
        
    }
}