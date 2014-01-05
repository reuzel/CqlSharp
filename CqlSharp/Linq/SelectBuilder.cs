// CqlSharp - CqlSharp
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
using System.Linq.Expressions;
using CqlSharp.Linq.Expressions;

namespace CqlSharp.Linq
{
    internal class SelectBuilder : BuilderBase
    {
        public ProjectionExpression UpdateSelect(ProjectionExpression projection, Expression selectExpression)
        {
            //get the lambda expression of the select method
            var lambda = (LambdaExpression) selectExpression.StripQuotes();

            //map the arguments of the lambda expression to the existing projection
            MapLambdaParameters(lambda, projection.Projection);

            //get the new projection
            var newProjection = Visit(lambda.Body);

            //check if projection is actually changed
            if (newProjection == lambda.Body)
                return projection;

            //get used columns
            IEnumerable<SelectorExpression> columns = new ColumnFinder().FindColumns(newProjection);

            SelectStatementExpression select = projection.Select;
            var newSelect = new SelectStatementExpression(select.Type,
                                                          new SelectClauseExpression(columns.ToArray(),
                                                                                     select.SelectClause.Distinct),
                                                          select.TableName, select.WhereClause, select.OrderBy,
                                                          select.Limit);

            return new ProjectionExpression(newSelect, newProjection, projection.ResultFunction);
        }

        #region Nested type: ColumnFinder

        /// <summary>
        ///   finds used column identifiers for the select clause
        /// </summary>
        private class ColumnFinder : CqlExpressionVisitor
        {
            private readonly HashSet<SelectorExpression> _identifiers = new HashSet<SelectorExpression>();

            public IEnumerable<SelectorExpression> FindColumns(Expression expression)
            {
                Visit(expression);
                return _identifiers;
            }

            public override Expression VisitIdentifier(IdentifierExpression identifier)
            {
                _identifiers.Add(new SelectorExpression(identifier));
                return identifier;
            }
        }

        #endregion
    }
}