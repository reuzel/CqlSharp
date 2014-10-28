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
using System.Reflection;

namespace CqlSharp.Linq.Query
{
    /// <summary>
    ///   Translates a Linq Expression tree into a Cql expression tree
    /// </summary>
    internal class ExpressionTranslator : BuilderBase
    {
        private static readonly MethodInfo GenericSelectMethod = typeof(Enumerable).GetMethods()
                                                            .Where(m => m.Name == "Select")
                                                            .First(m => m.GetParameters()[1].ParameterType.GenericTypeArguments.Length == 2);

        public ExpressionTranslator()
            : base(new Dictionary<Expression, Expression>())
        {

        }

        public ProjectionExpression Translate(Expression expression)
        {
            var visited = Visit(expression);

            var translation = visited as ProjectionExpression;
            if (translation == null)
                throw new CqlLinqException("Unexpected expression encountered: " + visited.NodeType.ToString());

            return translation;
        }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            var lamda = (LambdaExpression)node.StripQuotes();

            Map.Add(lamda.Parameters[0], new DatabaseExpression(lamda.Parameters[0].Type));
            for (int i = 1; i < lamda.Parameters.Count; i++)
                Map.Add(lamda.Parameters[i], new TermExpression(lamda.Parameters[i].Type, i - 1));

            return Visit(lamda.Body);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            //replace parameter with corresponding projection if known
            Expression projection;
            if (Map.TryGetValue(node, out projection))
                return projection;

            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            var source = Visit(node.Expression) as DatabaseExpression;
            var tableType = node.Member is FieldInfo
                           ? ((FieldInfo)node.Member).FieldType
                           : ((PropertyInfo)node.Member).PropertyType;

            if (!tableType.Implements(typeof(ICqlTable)))
                throw new CqlLinqException(string.Format("Accesssed member {0} is not a table", node.Member.Name));

            var table = (ICqlTable)Activator.CreateInstance(tableType, true);
            return CreateTableProjection(table);
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
            var selectors = new List<SelectorExpression>();
            var bindings = new List<MemberBinding>();
            foreach (var column in table.Columns)
            {
                var identifierType = column.Type;

                var identifierName = column.Name;

                var identifier = new SelectorExpression(identifierName, identifierType);

                selectors.Add(identifier);

                bindings.Add(Expression.Bind(column.MemberInfo, identifier));
            }

            var selectClause = new SelectClauseExpression(selectors, false);
            var selectStmt = new SelectStatementExpression(typeof(IEnumerable<>).MakeGenericType(table.EntityType), selectClause, table.Name, null, null, null, false);

            var projection = Expression.MemberInit(Expression.New(table.EntityType), bindings);

            return new ProjectionExpression(selectStmt, projection, null, true, null, null);
        }



        protected override Expression VisitMethodCall(MethodCallExpression call)
        {
            if (call.Method.DeclaringType == typeof(CqlContext))
            {
                switch (call.Method.Name)
                {
                    case "GetTable":
                        {
                            var entityType = call.Method.GetGenericArguments()[0];
                            var tableType = typeof(CqlTable<>).MakeGenericType(entityType);

                            var table = (ICqlTable)Activator.CreateInstance(tableType, true);
                            return CreateTableProjection(table);
                        }

                }
            }
            if (call.Method.DeclaringType == typeof(CqlQueryable))
            {
                switch (call.Method.Name)
                {
                    case "AllowFiltering":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            var select = new SelectStatementExpression(source.Select.Type,
                                                                       source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause,
                                                                       source.Select.OrderBy,
                                                                       source.Select.Limit,
                                                                       true);

                            return new ProjectionExpression(@select,
                                                            source.Projection,
                                                            source.Aggregator,
                                                            source.CanTrackChanges,
                                                            source.Consistency,
                                                            source.PageSize);
                        }

                    case "AsNoTracking":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            return new ProjectionExpression(source.Select,
                                                            source.Projection,
                                                            source.Aggregator,
                                                            false,
                                                            source.Consistency,
                                                            source.PageSize);
                        }

                    case "WithPageSize":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);
                            var size = (int)((ConstantExpression)call.Arguments[1]).Value;

                            return new ProjectionExpression(source.Select,
                                                            source.Projection,
                                                            source.Aggregator,
                                                            source.CanTrackChanges,
                                                            source.Consistency,
                                                            size);
                        }

                    case "WithConsistency":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);
                            var consistency = (CqlConsistency)((ConstantExpression)call.Arguments[1]).Value;

                            return new ProjectionExpression(source.Select,
                                                            source.Projection,
                                                            source.Aggregator,
                                                            source.CanTrackChanges,
                                                            consistency,
                                                            source.PageSize);
                        }
                }
            }
            else if (call.Method.DeclaringType == typeof(Queryable) || call.Method.DeclaringType == typeof(Enumerable))
            {
                switch (call.Method.Name)
                {
                    case "ToList":
                    case "ToDictionary":
                    case "ToLookup":
                    case "ToArray":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            var parameter = Expression.Parameter(source.Type, "resultSet");

                            //get the argument list, and replace the original source with the resultSet parameter
                            var arguments = new List<Expression>(call.Arguments);
                            arguments[0] = parameter;

                            //call the method (e.g. ToDictionary) and cast result to object
                            var toCall = Expression.Call(call.Method, arguments);
                            var aggregator = Expression.Lambda(toCall, parameter);

                            return new ProjectionExpression(source.Select, source.Projection, aggregator, source.CanTrackChanges, source.Consistency, source.PageSize);
                        }

                    case "Select":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);
                            return new SelectBuilder(Map).UpdateSelect(source, call.Arguments[1]);
                        }

                    case "Where":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            if (source.Select.Limit.HasValue)
                                throw new CqlLinqException(
                                    "A Where statement may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the Take after the Where statement.");

                            return new WhereBuilder(Map).BuildWhere(source, call.Arguments[1]);
                        }

                    case "Distinct":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            //make sure limit is not set yet (as otherwise the semantics of the query cannot be supported)
                            if (source.Select.Limit.HasValue)
                                throw new CqlLinqException("Any Take operation most occur after Distinct");

                            //set distinct on the select clause
                            var selectClause = new SelectClauseExpression(source.Select.SelectClause.Selectors, true);

                            //update select
                            var select = new SelectStatementExpression(source.Select.Type,
                                                                       selectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause,
                                                                       source.Select.OrderBy,
                                                                       source.Select.Limit,
                                                                       source.Select.AllowFiltering);

                            //update projection
                            return new ProjectionExpression(@select,
                                                            source.Projection,
                                                            source.Aggregator,
                                                            false,
                                                            source.Consistency,
                                                            source.PageSize);
                        }

                    case "Take":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            //get take
                            var take = (int)((ConstantExpression)call.Arguments[1]).Value;

                            //use minimum of takes...
                            take = source.Select.Limit.HasValue ? Math.Min(source.Select.Limit.Value, take) : take;

                            //add limit to return given amount of results
                            var select = new SelectStatementExpression(source.Select.Type,
                                                                       source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause,
                                                                       source.Select.OrderBy,
                                                                       take,
                                                                       source.Select.AllowFiltering);

                            return new ProjectionExpression(@select,
                                                            source.Projection,
                                                            source.Aggregator,
                                                            source.CanTrackChanges,
                                                            source.Consistency,
                                                            source.PageSize);
                        }

                    case "First":
                    case "FirstOrDefault":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                            {
                                if (source.Select.Limit.HasValue)
                                    throw new CqlLinqException(
                                        "A First statement with a condition may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the condition into a Where clause executed before the Take.");

                                source = new WhereBuilder(Map).BuildWhere(source, call.Arguments[1]);
                            }

                            //add limit to return single result
                            var select = new SelectStatementExpression(source.Select.Type,
                                                                       source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause,
                                                                       source.Select.OrderBy,
                                                                       1,
                                                                       source.Select.AllowFiltering);

                            //use Enumerable logic for processing result set
                            var parameter = Expression.Parameter(source.Type, "resultSet");
                            var first = Expression.Call(typeof(Enumerable), call.Method.Name, new[] { source.Projection.Type }, parameter);
                            var aggregator = Expression.Lambda(first, parameter);

                            return new ProjectionExpression(@select,
                                                            source.Projection,
                                                            aggregator,
                                                            source.CanTrackChanges,
                                                            source.Consistency,
                                                            null);
                        }

                    case "Single":
                    case "SingleOrDefault":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                            {
                                if (source.Select.Limit.HasValue)
                                    throw new CqlLinqException(
                                        "A Single statement with a condition may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the condition into a Where clause executed before the Take.");

                                source = new WhereBuilder(Map).BuildWhere(source, call.Arguments[1]);
                            }

                            //set the limit to min of current limit or 2
                            int limit = source.Select.Limit.HasValue ? Math.Min(source.Select.Limit.Value, 2) : 2;

                            //add limit to return single result
                            var select = new SelectStatementExpression(source.Select.Type,
                                                                       source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause,
                                                                       source.Select.OrderBy,
                                                                       limit,
                                                                       source.Select.AllowFiltering);

                            //use Enumerable logic for processing result set
                            var parameter = Expression.Parameter(source.Type, "resultSet");
                            var single = Expression.Call(typeof(Enumerable), call.Method.Name, new[] { source.Projection.Type }, parameter);
                            var aggregator = Expression.Lambda(single, parameter);

                            return new ProjectionExpression(@select,
                                                            source.Projection,
                                                            aggregator,
                                                            source.CanTrackChanges,
                                                            source.Consistency,
                                                            null);
                        }

                    case "Any":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                            {
                                if (source.Select.Limit.HasValue)
                                    throw new CqlLinqException(
                                        "An Any statement with a condition may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the condition into a Where clause executed before the Take.");

                                source = new WhereBuilder(Map).BuildWhere(source, call.Arguments[1]);
                            }

                            //count the items, with limit 1 (as single hit is enough)
                            var select = new SelectStatementExpression(typeof(IEnumerable<long>),
                                                                       new SelectClauseExpression(true),
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause,
                                                                       null,
                                                                       1,
                                                                       source.Select.AllowFiltering);

                            //check if count > 0
                            var parameter = Expression.Parameter(typeof(IEnumerable<long>), "resultSet");
                            var evaluation = Expression.GreaterThan(Expression.Call(typeof(Enumerable), "SingleOrDefault", new[] { typeof(long) }, parameter), Expression.Constant(0L));
                            var aggregator = Expression.Lambda(evaluation, parameter);

                            return new ProjectionExpression(@select, new SelectorExpression("count", typeof(long)), aggregator, false, source.Consistency, null);
                        }

                    case "Count":
                    case "LongCount":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            //count and distinct do not go together in CQL
                            if (source.Select.SelectClause.Distinct)
                                throw new CqlLinqException("Count cannot be combined with Distinct in CQL");

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                            {
                                if (source.Select.Limit.HasValue)
                                    throw new CqlLinqException(
                                        "A Count statement with a condition may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the condition into a Where clause executed before the Take.");

                                source = new WhereBuilder(Map).BuildWhere(source, call.Arguments[1]);
                            }

                            //remove the select clause and replace with count(*)
                            var select = new SelectStatementExpression(typeof(IEnumerable<long>),
                                                                       new SelectClauseExpression(true),
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause,
                                                                       null,
                                                                       source.Select.Limit,
                                                                       source.Select.AllowFiltering);

                            //use Enumerable logic for processing result set
                            var parameter = Expression.Parameter(typeof(IEnumerable<long>), "resultSet");
                            Expression count = Expression.Call(typeof(Enumerable), "Single", new[] { typeof(long) }, parameter);
                            if (call.Method.Name.Equals("Count"))
                                count = Expression.Convert(count, typeof(int));
                            var aggregator = Expression.Lambda(count, parameter);

                            return new ProjectionExpression(@select, new SelectorExpression("count", typeof(long)), aggregator, false, source.Consistency, null);
                        }

                    case "OrderBy":
                    case "OrderByDescending":
                    case "ThenBy":
                    case "ThenByDescending":
                        {
                            if (call.Arguments.Count > 2)
                                throw new CqlLinqException(
                                    "Custom IComparer implementations are not supported in ordering expressions.");

                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            if (source.Select.Limit.HasValue)
                                throw new CqlLinqException(
                                    "An OrderBy or ThenBy statement may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the Take after the ordering statement.");

                            bool ascending = call.Method.Name.Equals("OrderBy") ||
                                             call.Method.Name.Equals("ThenBy");

                            return new OrderBuilder(Map).UpdateOrder(source, call.Arguments[1], ascending);
                        }

                }
            }

            throw new CqlLinqException(string.Format("Method {0} is not supported", call.Method));
        }
    }
}