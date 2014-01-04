using CqlSharp.Linq.Expressions;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace CqlSharp.Linq
{
    class SelectBuilder : BuilderBase
    {
        public ProjectionExpression UpdateSelect(ProjectionExpression projection, Expression selectExpression)
        {
            //get the lambda expression of the select method
            var lambda = (LambdaExpression)selectExpression.StripQuotes();

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
            var newSelect = new SelectStatementExpression(select.Type, new SelectClauseExpression(columns.ToArray(), select.SelectClause.Distinct),
                                                          select.TableName, select.WhereClause, select.OrderBy,
                                                          select.Limit);

            return new ProjectionExpression(newSelect, newProjection, projection.ResultFunction);
        }

        /// <summary>
        /// finds used column identifiers for the select clause
        /// </summary>
        class ColumnFinder : CqlExpressionVisitor
        {
            readonly HashSet<SelectorExpression> _identifiers = new HashSet<SelectorExpression>();

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
    }
}
