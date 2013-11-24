using CqlSharp.Linq.Expressions;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace CqlSharp.Linq
{
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

            return new ProjectionExpression(selectStmt, projection);
        }

        protected override Expression VisitMethodCall(MethodCallExpression call)
        {
            if (call.Method.DeclaringType == typeof(Queryable))
            {
                switch (call.Method.Name)
                {
                    case "Select":
                        {
                            //visit the source of the select first. This should return a ProjectionExpression
                            //as it would be either a table, another select or where clause that return
                            //ProjectionExpressions themselves
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);
                            return new SelectBuilder().UpdateSelect(source, call.Arguments[1]);
                        }
                    case "Where":
                        {
                            var source = (ProjectionExpression)Visit(call.Arguments[0]);
                            return new WhereBuilder().BuildWhere(source, call.Arguments[1]);
                        }
                    default:
                        throw new CqlLinqException(string.Format("Method {0} is not supported", call.Method));
                }
            }

            return call;
        }

    }
}
