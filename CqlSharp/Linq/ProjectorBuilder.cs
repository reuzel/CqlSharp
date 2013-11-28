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
            var value = Expression.MakeIndex(_reader, Indexer, new[] { Expression.Constant(identifier.Name) });

            if (identifier.Type.IsValueType)
            {
                return Expression.Condition(
                    Expression.Equal(value, Expression.Constant(null)),
                    Expression.Default(identifier.Type),
                    Expression.Convert(value, identifier.Type));
            }

            return Expression.Convert(value, identifier.Type);
        }
    }
}
