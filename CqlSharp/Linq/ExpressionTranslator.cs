using CqlSharp.Linq.Expressions;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace CqlSharp.Linq
{
    /// <summary>
    /// Translates a Linq Expression tree into a Cql expression tree
    /// </summary>
    class ExpressionTranslator : CqlExpressionVisitor
    {
        private readonly Dictionary<Expression, Expression> _map = new Dictionary<Expression, Expression>();

        public ProjectionExpression Translate(Expression expression)
        {
            return (ProjectionExpression)Visit(expression);
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
        /// Creates the table projection.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns></returns>
        private static Expression CreateTableProjection(ICqlTable table)
        {
            var enumType = typeof(IEnumerable<>).MakeGenericType(table.Type);

            var selectors = new List<SelectorExpression>();
            var bindings = new List<MemberBinding>();
            foreach (var column in table.ColumnNames)
            {
                var identifierType = column.Key.MemberType == MemberTypes.Property
                                         ? ((PropertyInfo)column.Key).PropertyType
                                         : ((FieldInfo)column.Key).FieldType;

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
            if (call.Method.DeclaringType == typeof(Queryable))
            {
                switch (call.Method.Name)
                {
                    case "Select":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);
                            var newProjection = new SelectBuilder().UpdateSelect(source, call.Arguments[1]);
                            return new ColumnReducer().ReduceColumns(newProjection);
                        }
                    case "Where":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);
                            return new WhereBuilder().BuildWhere(source, call.Arguments[1]);
                        }
                    case "First":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                                source = new WhereBuilder().BuildWhere(source, call.Arguments[1]);

                            //add limit to return single result
                            var select = new SelectStatementExpression(source.Select.Type, source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause, source.Select.OrderBy,
                                                                       1);

                            return new ProjectionExpression(select, source.Projection, Enumerable.First);
                        }
                    case "FirstOrDefault":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                                source = new WhereBuilder().BuildWhere(source, call.Arguments[1]);

                            //add limit to return single result
                            var select = new SelectStatementExpression(source.Select.Type, source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause, source.Select.OrderBy,
                                                                       1);

                            return new ProjectionExpression(select, source.Projection, Enumerable.FirstOrDefault);
                        }
                    case "Single":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                                source = new WhereBuilder().BuildWhere(source, call.Arguments[1]);

                            //add limit to return single result
                            var select = new SelectStatementExpression(source.Select.Type, source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause, source.Select.OrderBy,
                                                                       2);

                            return new ProjectionExpression(select, source.Projection, Enumerable.Single);
                        }
                    case "SingleOrDefault":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                                source = new WhereBuilder().BuildWhere(source, call.Arguments[1]);

                            //add limit to return single result
                            var select = new SelectStatementExpression(source.Select.Type, source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause, source.Select.OrderBy,
                                                                       2);

                            return new ProjectionExpression(select, source.Projection, Enumerable.SingleOrDefault);
                        }

                    case "Any":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                                source = new WhereBuilder().BuildWhere(source, call.Arguments[1]);

                            //add limit to return single result
                            var select = new SelectStatementExpression(source.Select.Type, source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause, source.Select.OrderBy,
                                                                       1);

                            return new ProjectionExpression(select, source.Projection, (enm) => enm.Any());
                        }

                    case "Count":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                                source = new WhereBuilder().BuildWhere(source, call.Arguments[1]);

                            //remove the select clause and replace with count(*)
                            var select = new SelectStatementExpression(typeof(long), new SelectClauseExpression(true),
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause, null, null);

                            return new ProjectionExpression(select, new IdentifierExpression(typeof(long), "count"), Enumerable.Single);
                        }

                    default:
                        throw new CqlLinqException(string.Format("Method {0} is not supported", call.Method));
                }
            }

            return call;
        }

    }
}
