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

using CqlSharp.Linq.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace CqlSharp.Linq.Query
{
    internal class SelectBuilder : BuilderBase
    {
        public SelectBuilder(Dictionary<Expression, Expression> parameterMap)
            : base(parameterMap)
        { }

        public ProjectionExpression UpdateSelect(ProjectionExpression projection, Expression selectExpression)
        {
            //get the lambda expression of the select method
            var lambda = (LambdaExpression)selectExpression.StripQuotes();

            //if the lambda, is the identity lamda, simply return
            if (lambda.IsIdentityLambda())
                return projection;

            //map the source argument of the lambda expression to the existing projection
            Map.Add(lambda.Parameters[0], projection.Projection);

            //get the new projection
            var newProjection = Visit(lambda.Body);

            //check if projection is actually changed
            if (newProjection == lambda.Body)
                return projection;

            //get used columns
            IEnumerable<SelectorExpression> columns = new ColumnFinder().FindColumns(newProjection);

            SelectStatementExpression select = projection.Select;
            var newSelect = new SelectStatementExpression(typeof(IEnumerable<>).MakeGenericType(newProjection.Type),
                                                          new SelectClauseExpression(columns.ToArray(),
                                                                                     select.SelectClause.Distinct),
                                                          select.TableName,
                                                          select.WhereClause,
                                                          select.OrderBy,
                                                          select.Limit,
                                                          select.AllowFiltering);
            
            return new ProjectionExpression(newSelect, newProjection, projection.Aggregator, false, projection.Consistency, projection.PageSize);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            Type classType = node.Method.DeclaringType;
            if (classType == typeof(CqlFunctions))
            {
                bool changed;
                var args = node.Arguments.VisitAll(this, out changed);
                if (args.Any(arg => arg.GetType() != typeof(SelectorExpression)))
                    throw new CqlLinqException(
                        string.Format("Argument to {0} function is not recognized as a column identifier",
                                      node.Method.Name));

                return new SelectorExpression(node.Method, args.Cast<SelectorExpression>());
            }

            return base.VisitMethodCall(node);
        }

        #region Nested type: ColumnFinder

        /// <summary>
        ///   finds used selectors for the select clause
        /// </summary>
        private class ColumnFinder : CqlExpressionVisitor
        {
            private readonly HashSet<SelectorExpression> _selectors = new HashSet<SelectorExpression>();

            public IEnumerable<SelectorExpression> FindColumns(Expression expression)
            {
                Visit(expression);
                return _selectors;
            }

            public override Expression VisitSelector(SelectorExpression selector)
            {
                _selectors.Add(selector);
                return selector;
            }
        }

        #endregion
    }
}