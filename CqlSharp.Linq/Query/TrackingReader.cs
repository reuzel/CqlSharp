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
using System.Collections.Generic;

namespace CqlSharp.Linq.Query
{
    internal class TrackingReader<TEntity> : ProjectionReader<TEntity> where TEntity : class, new()
    {
        private readonly TableChangeTracker<TEntity> _tracker;

        /// <summary>
        /// Initializes a new instance of the <see cref="ProjectionReader{T}" /> class.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="plan">The plan.</param>
        /// <param name="args">The arguments to fill the parameters of the (prepared) query</param>
        /// <exception cref="System.ArgumentNullException">context
        /// or
        /// cql
        /// or
        /// projector</exception>
        public TrackingReader(CqlContext context, QueryPlan<TEntity> plan, object[] args)
            : base(context, plan, args)
        {
            _tracker = context.ChangeTracker.GetTableChangeTracker<TEntity>();
        }

        #region IEnumerable<TEntity> Members

        /// <summary>
        ///   Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns> A <see cref="T:System.Collections.Generic.IEnumerator`1" /> that can be used to iterate through the collection. </returns>
        public override IEnumerator<TEntity> GetEnumerator()
        {
            var enumerator = base.GetEnumerator();
            while (enumerator.MoveNext())
            {
                yield return _tracker.GetOrAttach(enumerator.Current);
            }
        }

        #endregion
    }
}