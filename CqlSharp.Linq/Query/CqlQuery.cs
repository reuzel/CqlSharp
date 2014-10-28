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
using System.Linq;
using System.Linq.Expressions;

namespace CqlSharp.Linq.Query
{
    /// <summary>
    ///   Represents a Cql Linq query
    /// </summary>
    /// <typeparam name="TElement"> </typeparam>
    public class CqlQuery<TElement> : IOrderedQueryable<TElement>
    {
        private readonly Expression _expression;
        private readonly IQueryProvider _provider;

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlQuery{T}" /> class.
        /// </summary>
        /// <param name="provider"> The provider. </param>
        internal CqlQuery(IQueryProvider provider)
        {
            _provider = provider;
            _expression = Expression.Constant(this);
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlQuery{T}" /> class.
        /// </summary>
        /// <param name="provider"> The provider. </param>
        /// <param name="expression"> The expression. </param>
        internal CqlQuery(IQueryProvider provider, Expression expression)
        {
            _provider = provider;
            _expression = expression;
        }

        #region IOrderedQueryable<TElement> Members

        /// <summary>
        ///   Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns> A <see cref="T:System.Collections.Generic.IEnumerator`1" /> that can be used to iterate through the collection. </returns>
        public IEnumerator<TElement> GetEnumerator()
        {
            return ((IEnumerable<TElement>) Provider.Execute(_expression)).GetEnumerator();
        }

        /// <summary>
        ///   Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns> An <see cref="T:System.Collections.IEnumerator" /> object that can be used to iterate through the collection. </returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        ///   Gets the type of the element(s) that are returned when the expression tree associated with this instance of <see
        ///    cref="T:System.Linq.IQueryable" /> is executed.
        /// </summary>
        /// <returns> A <see cref="T:System.Type" /> that represents the type of the element(s) that are returned when the expression tree associated with this object is executed. </returns>
        public Type ElementType
        {
            get { return typeof (TElement); }
        }

        /// <summary>
        ///   Gets the expression tree that is associated with the instance of <see cref="T:System.Linq.IQueryable" />.
        /// </summary>
        /// <returns> The <see cref="T:System.Linq.Expressions.Expression" /> that is associated with this instance of <see
        ///    cref="T:System.Linq.IQueryable" /> . </returns>
        public Expression Expression
        {
            get { return _expression; }
        }

        /// <summary>
        ///   Gets the query provider that is associated with this data source.
        /// </summary>
        /// <returns> The <see cref="T:System.Linq.IQueryProvider" /> that is associated with this data source. </returns>
        public IQueryProvider Provider
        {
            get { return _provider; }
        }

        #endregion

        public override string ToString()
        {
            return string.Format("CqlTable<{0}>", ElementType.Name);
        }
    }
}