using System.Linq;
using System.Reflection;
using CqlSharp.Linq.Expressions;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace CqlSharp.Linq
{
    class CqlProjectionVisitor : CqlExpressionVisitor
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
            var enumType = typeof (IEnumerable<>).MakeGenericType(table.Type);

            var selectors = new List<Expression>();
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

            var selectClause = new SelectClauseExpression(selectors);
            var selectStmt = new SelectStatementExpression(enumType, selectClause, table.Name, null, null, null);

            var projection = Expression.MemberInit(Expression.New(table.Type), bindings);

            return new ProjectionExpression(selectStmt, projection);
        }
    }
}
