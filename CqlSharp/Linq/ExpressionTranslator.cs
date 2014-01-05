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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using CqlSharp.Linq.Expressions;

namespace CqlSharp.Linq
{
    /// <summary>
    ///   Translates a Linq Expression tree into a Cql expression tree
    /// </summary>
    internal class ExpressionTranslator : CqlExpressionVisitor
    {
        private readonly Dictionary<Expression, Expression> _map = new Dictionary<Expression, Expression>();

        public ProjectionExpression Translate(Expression expression)
        {
            return (ProjectionExpression) Visit(expression);
        }

        protected override Expression VisitConstant(ConstantExpression constant)
        {
            var table = constant.Value as ICqlTable;
            if (table != null)
            {
                return CreateTableProjection(table);
            }

            return base.VisitConstant(constant);
        }

        /// <summary>
        ///   Creates the table projection.
        /// </summary>
        /// <param name="table"> The table. </param>
        /// <returns> </returns>
        private static Expression CreateTableProjection(ICqlTable table)
        {
            var enumType = typeof (IEnumerable<>).MakeGenericType(table.Type);

            var selectors = new List<SelectorExpression>();
            var bindings = new List<MemberBinding>();
            foreach (var column in table.ColumnNames)
            {
                var identifierType = column.Key.MemberType == MemberTypes.Property
                                         ? ((PropertyInfo) column.Key).PropertyType
                                         : ((FieldInfo) column.Key).FieldType;

                var identifierName = column.Value;

                var identifier = new IdentifierExpression(identifierType, identifierName);

                selectors.Add(new SelectorExpression(identifier));

                bindings.Add(Expression.Bind(column.Key, identifier));
            }

            var selectClause = new SelectClauseExpression(selectors, false);
            var selectStmt = new SelectStatementExpression(enumType, selectClause, table.Name, null, null, null);

            var projection = Expression.MemberInit(Expression.New(table.Type), bindings);

            return new ProjectionExpression(selectStmt, projection, null);
        }

        protected override Expression VisitMethodCall(MethodCallExpression call)
        {
            if (call.Method.DeclaringType == typeof (Queryable))
            {
                switch (call.Method.Name)
                {
                    case "Select":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);
                            return new SelectBuilder().UpdateSelect(source, call.Arguments[1]);
                        }

                    case "Where":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            if (source.Select.Limit.HasValue)
                                throw new CqlLinqException(
                                    "A Where statement may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the Take after the Where statement.");

                            return new WhereBuilder().BuildWhere(source, call.Arguments[1]);
                        }

                    case "Take":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            //get take
                            var take = (int) ((ConstantExpression) call.Arguments[1]).Value;

                            //use minimum of takes...
                            take = source.Select.Limit.HasValue ? Math.Min(source.Select.Limit.Value, take) : take;

                            //add limit to return single result

                            var select = new SelectStatementExpression(source.Select.Type, source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause, source.Select.OrderBy,
                                                                       take);

                            return new ProjectionExpression(select, source.Projection, source.ResultFunction);
                        }

                    case "First":
                    case "FirstOrDefault":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                            {
                                if (source.Select.Limit.HasValue)
                                    throw new CqlLinqException(
                                        "A First statement with a condition may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the condition into a Where clause executed before the Take.");

                                source = new WhereBuilder().BuildWhere(source, call.Arguments[1]);
                            }

                            //add limit to return single result
                            var select = new SelectStatementExpression(source.Select.Type, source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause, source.Select.OrderBy,
                                                                       1);

                            //use Enumerable logic for processing result set
                            ResultFunction processor = call.Method.Name.Equals("First")
                                                           ? Enumerable.First
                                                           : (ResultFunction) Enumerable.FirstOrDefault;

                            return new ProjectionExpression(select, source.Projection, processor);
                        }

                    case "Single":
                    case "SingleOrDefault":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                            {
                                if (source.Select.Limit.HasValue)
                                    throw new CqlLinqException(
                                        "A Single statement with a condition may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the condition into a Where clause executed before the Take.");

                                source = new WhereBuilder().BuildWhere(source, call.Arguments[1]);
                            }

                            //set the limit to min of current limit or 2
                            int limit = source.Select.Limit.HasValue ? Math.Min(source.Select.Limit.Value, 2) : 2;

                            //add limit to return single result
                            var select = new SelectStatementExpression(source.Select.Type, source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause, source.Select.OrderBy,
                                                                       limit);

                            //use Enumerable logic for processing result set
                            ResultFunction processor = call.Method.Name.Equals("Single")
                                                           ? Enumerable.Single
                                                           : (ResultFunction) Enumerable.SingleOrDefault;


                            return new ProjectionExpression(select, source.Projection, processor);
                        }

                    case "Any":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                            {
                                if (source.Select.Limit.HasValue)
                                    throw new CqlLinqException(
                                        "An Any statement with a condition may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the condition into a Where clause executed before the Take.");

                                source = new WhereBuilder().BuildWhere(source, call.Arguments[1]);
                            }

                            //add limit to return single result
                            var select = new SelectStatementExpression(source.Select.Type, source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause, null,
                                                                       1);

                            return new ProjectionExpression(select, source.Projection, (enm) => enm.Any());
                        }

                    case "Count":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                            {
                                if (source.Select.Limit.HasValue)
                                    throw new CqlLinqException(
                                        "A Count statement with a condition may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the condition into a Where clause executed before the Take.");

                                source = new WhereBuilder().BuildWhere(source, call.Arguments[1]);
                            }

                            //remove the select clause and replace with count(*)
                            var select = new SelectStatementExpression(typeof (int), new SelectClauseExpression(true),
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause, null,
                                                                       source.Select.Limit);

                            return new ProjectionExpression(select, new IdentifierExpression(typeof (int), "count"),
                                                            Enumerable.Single);
                        }


                    case "LongCount":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                            {
                                if (source.Select.Limit.HasValue)
                                    throw new CqlLinqException(
                                        "A Count statement with a condition may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the condition into a Where clause executed before the Take.");

                                source = new WhereBuilder().BuildWhere(source, call.Arguments[1]);
                            }

                            //remove the select clause and replace with count(*)
                            var select = new SelectStatementExpression(typeof (long), new SelectClauseExpression(true),
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause, null,
                                                                       source.Select.Limit);

                            return new ProjectionExpression(select, new IdentifierExpression(typeof (long), "count"),
                                                            Enumerable.Single);
                        }

                    case "OrderBy":
                    case "OrderByDescending":
                    case "ThenBy":
                    case "ThenByDescending":
                        {
                            if (call.Arguments.Count > 2)
                                throw new CqlLinqException(
                                    "Custom IComparer implementations are not supported in ordering expressions.");

                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            if (source.Select.Limit.HasValue)
                                throw new CqlLinqException(
                                    "An OrderBy or ThenBy statement may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the Take after the ordering statement.");

                            bool ascending = call.Method.Name.Equals("OrderBy") ||
                                             call.Method.Name.Equals("ThenBy");

                            return new OrderBuilder().UpdateOrder(source, call.Arguments[1], ascending);
                        }

                    default:
                        throw new CqlLinqException(string.Format("Method {0} is not supported", call.Method));
                }
            }

            return call;
        }
    }
}