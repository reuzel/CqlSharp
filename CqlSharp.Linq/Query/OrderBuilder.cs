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
using System.Collections.Generic;
using System.Linq.Expressions;

namespace CqlSharp.Linq.Query
{
    internal class OrderBuilder : BuilderBase
    {
        public OrderBuilder(Dictionary<Expression, Expression> parameterMap)
            : base(parameterMap)
        { }

        public ProjectionExpression UpdateOrder(ProjectionExpression projection, Expression keySelectExpression,
                                                bool ascending)
        {
            //get the lambda expression of the select method
            var lambda = (LambdaExpression)keySelectExpression.StripQuotes();

            //map the source argument of the lambda expression to the existing projection
            Map.Add(lambda.Parameters[0], projection.Projection);

            //get the new projection
            var key = Visit(lambda.Body);

            //check if we are dealing with a column
            if ((CqlExpressionType)key.NodeType != CqlExpressionType.IdentifierSelector)
                throw new CqlLinqException("Select key in OrderBy does not map to table column");

            //get a reference to the select clause
            SelectStatementExpression select = projection.Select;

            //add the ordering to the list of orderBy clauses
            var ordering = new List<OrderingExpression>(select.OrderBy);
            ordering.Add(new OrderingExpression((SelectorExpression)key,
                                                ascending
                                                    ? CqlExpressionType.OrderAscending
                                                    : CqlExpressionType.OrderDescending));

            var newSelect = new SelectStatementExpression(select.Type,
                                                          select.SelectClause,
                                                          select.TableName,
                                                          select.WhereClause,
                                                          ordering,
                                                          select.Limit,
                                                          select.AllowFiltering);

            return new ProjectionExpression(newSelect, projection.Projection,
                                            projection.Aggregator, projection.CanTrackChanges, projection.Consistency, projection.PageSize);
        }
    }
}