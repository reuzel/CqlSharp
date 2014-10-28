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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace CqlSharp.Linq.Query
{
    /// <summary>
    ///   Provider for the Cql Linq queries
    /// </summary>
    internal class CqlQueryProvider : IQueryProvider
    {
        private readonly CqlContext _cqlContext;


        internal CqlQueryProvider()
        {
            _cqlContext = null;
        }

        internal CqlQueryProvider(CqlContext cqlContext)
        {
            _cqlContext = cqlContext;
        }

        internal IQueryPlan CreateQueryPlan(Expression expression)
        {
            //evaluate all partial expressions (get rid of reference noise)
            var cleanedExpression = PartialEvaluator.Evaluate(expression, CanBeEvaluatedLocally);

            //translate the expression to a cql expression and corresponding projection
            var translation = new ExpressionTranslator().Translate(cleanedExpression);

            //generate cql text
            var cql = new CqlTextBuilder().Build(translation.Select);

            //get argument map (map from input arguments to command parameters)
            List<int> map = new VariableMapBuilder().BuildParameterMap(translation.Select);

            //get a projection delegate
            var projector = new ProjectorBuilder().BuildProjector(translation.Projection);

            //get a aggregator delegate
            var aggregator = translation.Aggregator != null ? translation.Aggregator.Compile() : null;

            //output some debug info
            Debug.WriteLine("Original Expression: " + expression);
            Debug.WriteLine("Cleaned Expression: " + cleanedExpression);
            Debug.WriteLine("Generated CQL: " + cql);
            Debug.WriteLine("Will track changes: " + translation.CanTrackChanges);
            if (translation.Consistency.HasValue)
                Debug.WriteLine("With Consistency: " + translation.Consistency);
            if (translation.PageSize.HasValue)
                Debug.WriteLine("With PageSize: " + translation.PageSize);
            Debug.WriteLine("Generated Projector: " + projector);
            Debug.WriteLine("Result processor: " +
                            (translation.Aggregator != null
                                 ? translation.Aggregator.ToString()
                                 : "<none>"));

            //return translation results
            return (IQueryPlan)Activator.CreateInstance(
                    typeof(QueryPlan<>).MakeGenericType(projector.ReturnType),
                    BindingFlags.Instance | BindingFlags.Public, null,
                    new object[] { cql, map, projector.Compile(), aggregator, translation.CanTrackChanges, translation.Consistency, translation.PageSize },
                    null);
        }

        private bool CanBeEvaluatedLocally(Expression expression)
        {
            var cex = expression as ConstantExpression;
            if (cex != null)
            {
                var query = cex.Value as IQueryable;
                if (query != null && query.Provider == this)
                    return false;
            }

            var mex = expression as MethodCallExpression;
            if (mex != null)
            {
                if (mex.Method.DeclaringType == typeof(CqlFunctions))
                    return false;
            }

            return expression.NodeType != ExpressionType.Parameter &&
                   expression.NodeType != ExpressionType.Lambda;
        }

        #region IQueryProvider implementation

        /// <summary>
        ///   Creates the query.
        /// </summary>
        /// <typeparam name="TElement"> The type of the element. </typeparam>
        /// <param name="expression"> The expression. </param>
        /// <returns> </returns>
        IQueryable<TElement> IQueryProvider.CreateQuery<TElement>(Expression expression)
        {
            return new CqlQuery<TElement>(this, expression);
        }

        /// <summary>
        ///   Constructs an <see cref="T:System.Linq.IQueryable" /> object that can evaluate the query represented by a specified expression tree.
        /// </summary>
        /// <param name="expression"> An expression tree that represents a LINQ query. </param>
        /// <returns> An <see cref="T:System.Linq.IQueryable" /> that can evaluate the query represented by the specified expression tree. </returns>
        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            Type elementType = TypeSystem.GetElementType(expression.Type);
            try
            {
                return
                    (IQueryable)
                    Activator.CreateInstance(typeof(CqlQuery<>).MakeGenericType(elementType),
                                             new object[] { this, expression });
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        /// <summary>
        ///   Executes the specified expression.
        /// </summary>
        /// <typeparam name="TResult"> The type of the result. </typeparam>
        /// <param name="expression"> The expression. </param>
        /// <returns> </returns>
        TResult IQueryProvider.Execute<TResult>(Expression expression)
        {
            var queryPlan = CreateQueryPlan(expression);
            return queryPlan.Execute<TResult>(_cqlContext, null);
        }

        /// <summary>
        ///   Executes the query represented by a specified expression tree.
        /// </summary>
        /// <param name="expression"> An expression tree that represents a LINQ query. </param>
        /// <returns> The value that results from executing the specified query. </returns>
        object IQueryProvider.Execute(Expression expression)
        {
            var queryPlan = CreateQueryPlan(expression);
            return queryPlan.Execute<object>(_cqlContext, null);
        }

        #endregion
    }
}