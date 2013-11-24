using CqlSharp.Linq.Expressions;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace CqlSharp.Linq
{
    class WhereBuilder : BuilderBase
    {
        private HashSet<RelationExpression> _relations = new HashSet<RelationExpression>();

        public ProjectionExpression BuildWhere(ProjectionExpression projection, Expression whereClause)
        {
            //get the lambda expression of the select method
            var lambda = (LambdaExpression)whereClause.StripQuotes();

            //map the arguments of the lambda expression to the existing projection
            MapLambdaParameters(lambda, projection.Projection);

            if(projection.Select.WhereClause!=null)
                _relations = new HashSet<RelationExpression>(projection.Select.WhereClause);
            else
                _relations = new HashSet<RelationExpression>();

            //get the new projections
            Expression expression = Visit(lambda.Body);

            if (!expression.IsTrue())
                throw new CqlLinqException("Where clause contains unsupported constructs");

            var select = projection.Select;

            var newSelectStmt = new SelectStatementExpression(select.Type, select.SelectClause, select.TableName,
                                                              _relations.ToArray(), select.OrderBy, select.Limit);

            return new ProjectionExpression(newSelectStmt, projection.Projection);
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            var comparison = (BinaryExpression)base.VisitBinary(node);

            switch (comparison.NodeType)
            {
                case ExpressionType.AndAlso:
                    return Expression.Constant(comparison.Left.IsTrue() && comparison.Right.IsTrue());
                case ExpressionType.Equal:
                    return CreateRelation(comparison.Left, comparison.Right, CqlExpressionType.Equal, CqlExpressionType.Equal);
                case ExpressionType.LessThanOrEqual:
                    return CreateRelation(comparison.Left, comparison.Right, CqlExpressionType.SmallerEqualThan, CqlExpressionType.LargerThan);
                case ExpressionType.LessThan:
                    return CreateRelation(comparison.Left, comparison.Right, CqlExpressionType.SmallerThan, CqlExpressionType.LargerEqualThan);
                case ExpressionType.GreaterThan:
                    return CreateRelation(comparison.Left, comparison.Right, CqlExpressionType.LargerThan, CqlExpressionType.SmallerEqualThan);
                case ExpressionType.GreaterThanOrEqual:
                    return CreateRelation(comparison.Left, comparison.Right, CqlExpressionType.LargerEqualThan, CqlExpressionType.SmallerThan);
            }

            throw new CqlLinqException(string.Format("CQL does not support the {0} operator", comparison.NodeType));
        }

        /// <summary>
        /// Shuffles identifier and term expression in the right order
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>true expression if the relation is succesfully created</returns>
        private Expression CreateRelation(Expression left, Expression right, CqlExpressionType compareOp, CqlExpressionType compareOpSwitched)
        {
            var leftType = (CqlExpressionType)left.NodeType;
            var rightType = (CqlExpressionType)right.NodeType;
            bool shuffled = false;
            if (rightType == CqlExpressionType.Identifier && leftType == CqlExpressionType.Constant)
            {
                Expression temp = left;
                left = right;
                right = temp;
                shuffled = true;
            }

            if (leftType == CqlExpressionType.Identifier && rightType == CqlExpressionType.Constant)
            {
                var relation = new RelationExpression((IdentifierExpression)left, shuffled ? compareOpSwitched : compareOp, (TermExpression)right);
                _relations.Add(relation);
                return Expression.Constant(true);
            }

            throw new CqlLinqException("Error creating relation. Not able to detect identifier and term");
        }

        /// <summary>
        /// Replaces constants with Cql Terms
        /// </summary>
        /// <param name="node">The expression to visit.</param>
        /// <returns>
        /// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
        /// </returns>
        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Type.IsGenericType)
            {
                if (node.Type.GetGenericTypeDefinition() == typeof(Dictionary<,>))
                    return new TermExpression(CqlExpressionType.Map, node.Value);

                if (node.Type.GetGenericTypeDefinition() == typeof(HashSet<>))
                    return new TermExpression(CqlExpressionType.Set, node.Value);

                if (node.Type.GetGenericTypeDefinition() == typeof(List<>))
                    return new TermExpression(CqlExpressionType.List, node.Value);
            }

            if (node.Type.IsArray)
            {
                return new TermExpression(CqlExpressionType.List, node.Value);
            }

            return new TermExpression(CqlExpressionType.Constant, node.Value);
        }

    }
}
