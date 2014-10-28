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
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace CqlSharp.Linq.Mutations
{
    internal class EntityHelper<TEntity>
    {
        public static EntityHelper<TEntity> Instance = new EntityHelper<TEntity>();

        private Func<TEntity, TEntity> _cloneFunction;
        private Func<TEntity, TEntity> _cloneKeyFunction;
        private Func<TEntity, TEntity, int> _copyToFunction;
        private Func<TEntity, int> _hashFunction;

        private EntityHelper()
        {
            SetCloneFunction();
            SetCloneKeyFunction();
            SetCopyToFunction();
            SetHashCodeFunction();
        }

        private void SetCloneFunction()
        {
            var source = Expression.Parameter(typeof(TEntity));
            var clone = Expression.New(typeof(TEntity));

            var bindings = new List<MemberBinding>();
            foreach (var column in ObjectAccessor<TEntity>.Instance.Columns)
            {
                var value = Expression.MakeMemberAccess(source, column.MemberInfo);
                bindings.Add(Expression.Bind(column.MemberInfo, value));
            }

            var cloneInit = Expression.MemberInit(clone, bindings);
            var lambda = Expression.Lambda(cloneInit, source);
            _cloneFunction = (Func<TEntity, TEntity>)lambda.Compile();
        }

        private void SetCloneKeyFunction()
        {
            var source = Expression.Parameter(typeof(TEntity));
            var clone = Expression.New(typeof(TEntity));

            var bindings = new List<MemberBinding>();
            var accessor = ObjectAccessor<TEntity>.Instance;
            foreach (var column in accessor.PartitionKeys.Concat(accessor.ClusteringKeys))
            {
                var value = Expression.MakeMemberAccess(source, column.MemberInfo);
                bindings.Add(Expression.Bind(column.MemberInfo, value));
            }

            var cloneInit = Expression.MemberInit(clone, bindings);
            var lambda = Expression.Lambda(cloneInit, source);
            _cloneKeyFunction = (Func<TEntity, TEntity>)lambda.Compile();
        }

        private void SetCopyToFunction()
        {
            var source = Expression.Parameter(typeof(TEntity));
            var target = Expression.Parameter(typeof(TEntity));

            var assignments = new List<Expression>();
            foreach (var column in ObjectAccessor<TEntity>.Instance.Columns)
            {
                var member = Expression.MakeMemberAccess(target, column.MemberInfo);
                var value = Expression.MakeMemberAccess(source, column.MemberInfo);
                assignments.Add(Expression.Assign(member, value));

            }
            assignments.Add(Expression.Constant(0));

            var assignblock = Expression.Block(assignments);
            var lambda = Expression.Lambda(assignblock, new[] { source, target });
            _copyToFunction = (Func<TEntity, TEntity, int>)lambda.Compile();
        }

        private void SetHashCodeFunction()
        {
            var source = Expression.Parameter(typeof(TEntity));

            var accessor = ObjectAccessor<TEntity>.Instance;
            Expression hashCode = Expression.Constant(17);

            foreach (var column in accessor.PartitionKeys.Concat(accessor.ClusteringKeys))
            {
                var hashMethod = column.Type.GetMethod("GetHashCode");
                var value = Expression.MakeMemberAccess(source, column.MemberInfo);
                var valueHash = Expression.Call(value, hashMethod);
                hashCode = Expression.Add(Expression.Multiply(hashCode, Expression.Constant(23)), valueHash);
            }
            var lambda = Expression.Lambda(hashCode, source);
            _hashFunction = (Func<TEntity, int>)lambda.Compile();
        }

        /// <summary>
        /// Returns a hash code for the provided entity, where the hashCode is  based on the entity key values
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns>
        /// A hash code for the entity, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public int GetHashCode(TEntity entity)
        {
            unchecked
            {
                return _hashFunction(entity);
            }
        }

        /// <summary>
        /// Clones the specified entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public TEntity Clone(TEntity entity)
        {
            return _cloneFunction(entity);
        }

        /// <summary>
        /// Clones the key properties and fields of the specified entity, into a new entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public TEntity CloneKey(TEntity entity)
        {
            return _cloneKeyFunction(entity);
        }

        /// <summary>
        /// Copies the value from the provided source to the provided destination.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="destination">The destination.</param>
        public void CopyTo(TEntity source, TEntity destination)
        {
            _copyToFunction(source, destination);
        }
    }
}