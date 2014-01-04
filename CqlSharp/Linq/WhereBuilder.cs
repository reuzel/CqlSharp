using CqlSharp.Linq.Expressions;
using System;
using System.Collections;
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

            if (projection.Select.WhereClause != null)
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

            return new ProjectionExpression(newSelectStmt, projection.Projection, projection.ResultFunction);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            Type classType = node.Method.DeclaringType;
            if (classType == typeof(Enumerable) || classType == typeof(Queryable))
            {
                Expression left = Visit(node.Arguments[0]);
                Expression right = Visit(node.Arguments[1]);

                switch (node.Method.Name)
                {
                    case "Equals":
                        return CreateRelation(left, right, CqlExpressionType.Equal,
                                              CqlExpressionType.Equal);
                    case "Contains":
                        return CreateRelation(left, right, CqlExpressionType.In, CqlExpressionType.In);
                }
            }

            if (node.Method.DeclaringType.Implements(typeof(IEnumerable<>)))
            {
                Expression left = Visit(node.Object);
                Expression right = Visit(node.Arguments[0]);
                switch (node.Method.Name)
                {
                    case "Contains":
                        return CreateRelation(left, right, CqlExpressionType.In, CqlExpressionType.In);
                }
            }

            throw new CqlLinqException(string.Format("Method {0} is not supported in CQL expressions", node.Method.Name));
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
        /// <param name="compareOp">The compare operation.</param>
        /// <param name="compareOpSwitched">The compare operation switched.</param>
        /// <returns>
        /// true expression if the relation is succesfully created
        /// </returns>
        /// <exception cref="CqlLinqException">
        /// Can't determine the (column) identfier for the CQL where relation
        /// or
        /// Error creating relation. Not able to detect the correct term to create the relation with.
        /// </exception>
        private Expression CreateRelation(Expression left, Expression right, CqlExpressionType compareOp, CqlExpressionType compareOpSwitched)
        {
            var leftType = (CqlExpressionType)left.NodeType;
            var rightType = (CqlExpressionType)right.NodeType;
            bool shuffled = false;

            //make sure identifier is left
            if (rightType == CqlExpressionType.Identifier)
            {
                //swap expressions
                Expression temp = left;
                left = right;
                right = temp;

                //swap type
                rightType = leftType;
                leftType = CqlExpressionType.Identifier;

                //indicate shuffling
                shuffled = true;
            }

            if (leftType != CqlExpressionType.Identifier)
                throw new CqlLinqException("Can't determine the (column) identfier for the CQL where relation");

            if (rightType == CqlExpressionType.Constant)
            {
                var relation = new RelationExpression((IdentifierExpression)left, shuffled ? compareOpSwitched : compareOp, (TermExpression)right);
                _relations.Add(relation);
                return Expression.Constant(true);
            }

            if (compareOp == CqlExpressionType.In && (rightType == CqlExpressionType.Set || rightType == CqlExpressionType.List))
            {
                var relation = new RelationExpression((IdentifierExpression)left, ((TermExpression)right).Terms);
                _relations.Add(relation);
                return Expression.Constant(true);
            }

            throw new CqlLinqException("Error creating relation. Not able to detect the correct term to create the relation with.");
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
            //check if it is a valid CQL type
            if (node.Type.IsCqlType())
                return new TermExpression(node.Value);

            //check if it is a map
            if (node.Type.Implements(typeof(IDictionary<,>)))
            {
                var terms = new Dictionary<TermExpression, TermExpression>();
                foreach (DictionaryEntry elem in (IDictionary)node.Value)
                    terms.Add(new TermExpression(elem.Key), new TermExpression(elem.Value));

                return new TermExpression(node.Type, terms);
            }

            //check if it is a set
            if (node.Type.Implements(typeof(ISet<>)))
            {
                var terms = new List<TermExpression>();
                foreach (var elem in (IEnumerable)node.Value)
                    terms.Add(new TermExpression(elem));

                return new TermExpression(node.Type, CqlExpressionType.Set, terms);
            }

            //check if it is a collection (and therefore will be represented as List)
            if (node.Type.Implements(typeof(IEnumerable<>)))
            {
                var terms = new List<TermExpression>();
                foreach (var elem in (IEnumerable)node.Value)
                    terms.Add(new TermExpression(elem));

                return new TermExpression(node.Type, CqlExpressionType.List, terms);
            }

            //hmmm, no valid mapping to a term can be made!
            throw new CqlLinqException(string.Format("Type {0} can't be coverted to a CQL constant, list, set or map", node.Type));
        }

        /// <summary>
        /// Performs simple type checking to see if new expression will result in valid CQL type
        /// </summary>
        /// <param name="node">The expression to visit.</param>
        /// <returns>
        /// The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.
        /// </returns>
        /// <exception cref="CqlLinqException"></exception>
        protected override Expression VisitNew(NewExpression node)
        {
            if (node.Type.IsCqlType() ||
                node.Type.Implements(typeof(ISet<>)) ||
                node.Type.Implements(typeof(IDictionary<,>)) ||
                node.Type.Implements(typeof(IEnumerable<>)))
                return base.VisitNew(node);

            throw new CqlLinqException(string.Format("Type {0} can not be converted to a valid CQL value", node.Type));
        }

    }
}
