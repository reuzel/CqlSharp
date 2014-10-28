using System;
using System.Linq.Expressions;

namespace CqlSharp.Linq.Expressions
{
    /// <summary>
    /// Represents a reference to a Database (context)
    /// </summary>
    internal class DatabaseExpression : Expression
    {
        private readonly Type _type;

        public DatabaseExpression(Type type)
        {
            _type = type;
        }

        public override Type Type
        {
            get { return _type; }
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)CqlExpressionType.Database; }
        }

        protected override Expression Accept(ExpressionVisitor visitor)
        {
            var type = visitor as CqlExpressionVisitor;

            if (type != null)
            {
                return type.VisitDatabase(this);
            }

            return base.Accept(visitor);
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            return this;
        }
    }
}