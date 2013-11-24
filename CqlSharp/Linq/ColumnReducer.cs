using CqlSharp.Linq.Expressions;
using System.Collections.Generic;
using System.Linq;

namespace CqlSharp.Linq
{
    /// <summary>
    /// Removes not used column identifiers from the select clause
    /// </summary>
    class ColumnReducer : CqlExpressionVisitor
    {
        readonly HashSet<SelectorExpression> _identifiers = new HashSet<SelectorExpression>();

        public ProjectionExpression ReduceColumns(ProjectionExpression expression)
        {
            Visit(expression.Projection);

            var select = (SelectStatementExpression)expression.Select;
            var newSelect = new SelectStatementExpression(select.Type, new SelectClauseExpression(_identifiers.ToArray(), select.SelectClause.Distinct),
                                                          select.TableName, select.WhereClause, select.OrderBy,
                                                          select.Limit);

            return new ProjectionExpression(newSelect, expression.Projection);
        }

        public override System.Linq.Expressions.Expression VisitIdentifier(IdentifierExpression identifier)
        {
            _identifiers.Add(new SelectorExpression(identifier));
            return identifier;
        }
    }
}
