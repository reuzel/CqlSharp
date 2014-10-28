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

using CqlSharp.Serialization;
using System;
using System.Diagnostics;
using System.Linq;

namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    ///   Defines a key from a table entry
    /// </summary>
    /// <typeparam name="TEntity"> The type of the entity. </typeparam>
    public struct EntityKey<TEntity> : IEntityKey where TEntity : class, new()
    {
        /// <summary>
        ///   The object from which the key is derived
        /// </summary>
        private readonly TEntity _entity;

        /// <summary>
        /// The hash code for this key. Cached to optimize performance
        /// </summary>
        private readonly int _hashCode;

        /// <summary>
        ///   Initializes a new instance of the <see cref="EntityKey{TEntity}" /> struct.
        /// </summary>
        /// <param name="entity"> The entity containing the key values. </param>
        internal EntityKey(TEntity entity)
        {
            Debug.Assert(entity != null);

            _entity = entity;
            _hashCode = CqlEntityComparer<TEntity>.Instance.GetHashCode(_entity);
        }

        /// <summary>
        ///   Gets the values that make up this key.
        /// </summary>
        /// <value> The values. </value>
        internal TEntity Values
        {
            get { return _entity; }
        }

        #region IEntityKey Members

        /// <summary>
        ///   Determines whether this key is the key of the specified entity.
        /// </summary>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        bool IEntityKey.IsKeyOf(object entity)
        {
            if (entity == null) return false;
            if (entity.GetType() != typeof(TEntity)) return false;
            return IsKeyOf((TEntity)entity);
        }

        /// <summary>
        ///   Determines whether the specified <see cref="System.Object"></see>, is equal to this instance.
        /// </summary>
        /// <param name="obj"> The <see cref="System.Object" /> to compare with this instance. </param>
        /// <returns> <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, <c>false</c> . </returns>
        bool IEntityKey.Equals(object obj)
        {
            return Equals(obj);
        }

        /// <summary>
        ///   Returns a hash code for this instance.
        /// </summary>
        /// <returns> A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. </returns>
        int IEntityKey.GetHashCode()
        {
            return _hashCode;
        }

        #endregion

        /// <summary>
        ///   Creates an EntityKey from the specified entity.
        /// </summary>
        /// <typeparam name="TEntity"> The type of the entity. </typeparam>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        internal static EntityKey<TEntity> Create(TEntity entity)
        {
            Debug.Assert(entity != null);

            var keyValues = EntityHelper<TEntity>.Instance.CloneKey(entity);
            return new EntityKey<TEntity>(keyValues);
        }

        /// <summary>
        ///   Creates an EntityKey from the specified key values.
        /// </summary>
        /// <typeparam name="TEntity"> The type of the entity. </typeparam>
        /// <param name="keyValues"> The key values. </param>
        /// <returns> </returns>
        /// <exception cref="System.ArgumentException">Not all required key values are provided
        ///   or
        ///   the types of the keyValues do not match the required types for the entity keys</exception>
        internal static EntityKey<TEntity> Create(params object[] keyValues)
        {
            var accessor = ObjectAccessor<TEntity>.Instance;
            var keyObject = Activator.CreateInstance<TEntity>();

            int index = 0;
            foreach (var keyColumn in accessor.PartitionKeys.Concat(accessor.ClusteringKeys))
            {
                if (index >= keyValues.Length)
                    throw new ArgumentException("Not all required key values are provided", "keyValues");

                if (keyValues[index].GetType() != keyColumn.Type)
                    throw new ArgumentException(
                        String.Format(
                            "The key value at index {0} has type {1}, which does not match the required key type {2} ",
                            index, keyValues[index].GetType().FullName, keyColumn.Type.FullName), "keyValues");

                keyColumn.Write(keyObject, keyValues[index++]);
            }

            return Create(keyObject);
        }

        /// <summary>
        ///   Determines whether this key represents the key of the specified entity.
        /// </summary>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        public bool IsKeyOf(TEntity entity)
        {
            if (entity == null) return false;
            return CqlEntityComparer<TEntity>.Instance.Equals(entity, _entity);
        }

        /// <summary>
        ///   Determines whether the specified <see cref="System.Object" />, is equal to this instance.
        /// </summary>
        /// <param name="obj"> The <see cref="System.Object" /> to compare with this instance. </param>
        /// <returns> <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, <c>false</c> . </returns>
        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            if (!(obj is EntityKey<TEntity>)) return false;

            return Equals((EntityKey<TEntity>)obj);
        }

        /// <summary>
        ///   Determines whether the specified <see cref="EntityKey{TEntity}" />, is equal to this instance.
        /// </summary>
        /// <param name="obj"> The <see cref="EntityKey{TEntity}" /> to compare with this instance. </param>
        /// <returns> <c>true</c> if the specified <see cref="EntityKey{TEntity}" /> is equal to this instance; otherwise, <c>false</c> . </returns>
        public bool Equals(EntityKey<TEntity> obj)
        {
            //false if hashCodes don't match
            if (_hashCode != obj._hashCode) return false;

            //check data with the comparer
            return CqlEntityComparer<TEntity>.Instance.Equals(_entity, obj._entity);
        }

        /// <summary>
        ///   Returns a hash code for this instance.
        /// </summary>
        /// <returns> A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. </returns>
        public override int GetHashCode()
        {
            return _hashCode;
        }

        public static bool operator ==(EntityKey<TEntity> first, EntityKey<TEntity> second)
        {
            return first.Equals(second);
        }

        public static bool operator !=(EntityKey<TEntity> first, EntityKey<TEntity> second)
        {
            return !(first.Equals(second));
        }
    }
}