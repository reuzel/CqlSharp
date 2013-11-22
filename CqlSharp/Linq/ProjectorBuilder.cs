using CqlSharp.Linq.Expressions;
using System.Linq.Expressions;
using System.Reflection;

namespace CqlSharp.Linq
{
    /// <summary>
    /// Converts an expression with identifier expressions to a lambda expression that takes a datareader as input.
    /// </summary>
    class ProjectorBuilder : CqlExpressionVisitor
    {
        private static readonly PropertyInfo Indexer = typeof(CqlDataReader).GetProperty("Item", new[] { typeof(string) });

        private ParameterExpression _reader;

        public LambdaExpression BuildProjector(Expression expression)
        {
            _reader = Expression.Parameter(typeof(CqlDataReader), "cqlDataReader");
            Expression expr = Visit(expression);
            return Expression.Lambda(expr, _reader);
        }

        public override Expression VisitIdentifier(IdentifierExpression identifier)
        {
            return Expression.Convert(Expression.MakeIndex(_reader, Indexer, new[] { Expression.Constant(identifier.Name) }), identifier.Type);
        }
    }
}
