// CqlSharp.Linq - CqlSharp.Linq
// Copyright (c) 2014 Joost Reuzel
//   
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   
// http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using CqlSharp.Linq.Expressions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace CqlSharp.Linq.Query
{
    internal class WhereBuilder : BuilderBase
    {
        private HashSet<RelationExpression> _relations = new HashSet<RelationExpression>();

        public WhereBuilder(Dictionary<Expression, Expression> parameterMap)
            : base(parameterMap)
        { }

        public ProjectionExpression BuildWhere(ProjectionExpression projection, Expression whereClause)
        {
            //get the lambda expression of the select method
            var lambda = (LambdaExpression)whereClause.StripQuotes();

            //map the source argument of the lambda expression to the existing projection (the projection is what the where clause queries)
            Map.Add(lambda.Parameters[0], projection.Projection);

            if (projection.Select.WhereClause != null)
                _relations = new HashSet<RelationExpression>(projection.Select.WhereClause);
            else
                _relations = new HashSet<RelationExpression>();

            //get the new projections
            Expression expression = Visit(lambda.Body);

            if (!expression.IsTrue())
                throw new CqlLinqException("Where clause contains unsupported constructs");

            var select = projection.Select;

            var newSelectStmt = new SelectStatementExpression(select.Type,
                                                              select.SelectClause,
                                                              select.TableName,
                                                              _relations.ToArray(),
                                                              select.OrderBy,
                                                              select.Limit,
                                                              select.AllowFiltering);

            return new ProjectionExpression(newSelectStmt, projection.Projection,
                                            projection.Aggregator, projection.CanTrackChanges, projection.Consistency, projection.PageSize);
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

            if (classType.Implements(typeof(ICollection<>)))
            {
                Expression left = Visit(node.Object);
                Expression right = Visit(node.Arguments[0]);
                switch (node.Method.Name)
                {
                    case "Contains":
                        return CreateRelation(left, right, CqlExpressionType.In, CqlExpressionType.In);
                }
            }

            if (classType == typeof(CqlFunctions))
            {
                if (node.Method.Name.Equals("TTL") || node.Method.Name.Equals("WriteTime"))
                    throw new CqlLinqException("TTL and WriteTime functions are not allowed in a where clause");

                bool changed;
                var args = node.Arguments.VisitAll(this, out changed).ToList();

                //if all args are selector expressions, then return a composite selector (token, etc.)
                if (args.All(arg => arg.GetType() == typeof(SelectorExpression)))
                {
                    return new SelectorExpression(node.Method, args.Cast<SelectorExpression>());
                }

                //not a selector, thus no selector types are allowed...
                if (args.Any(arg => arg.GetType() == typeof(SelectorExpression)))
                    throw new CqlLinqException(
                        string.Format("Function {0} may not a combination of column identifiers and constants",
                                      node.Method.Name));

                return new TermExpression(node.Method, args.Cast<TermExpression>());
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
                    return CreateRelation(comparison.Left, comparison.Right, CqlExpressionType.Equal,
                                          CqlExpressionType.Equal);
                case ExpressionType.LessThanOrEqual:
                    return CreateRelation(comparison.Left, comparison.Right, CqlExpressionType.SmallerEqualThan,
                                          CqlExpressionType.LargerThan);
                case ExpressionType.LessThan:
                    return CreateRelation(comparison.Left, comparison.Right, CqlExpressionType.SmallerThan,
                                          CqlExpressionType.LargerEqualThan);
                case ExpressionType.GreaterThan:
                    return CreateRelation(comparison.Left, comparison.Right, CqlExpressionType.LargerThan,
                                          CqlExpressionType.SmallerEqualThan);
                case ExpressionType.GreaterThanOrEqual:
                    return CreateRelation(comparison.Left, comparison.Right, CqlExpressionType.LargerEqualThan,
                                          CqlExpressionType.SmallerThan);
            }

            throw new CqlLinqException(string.Format("CQL does not support the {0} operator", comparison.NodeType));
        }

        /// <summary>
        ///   Shuffles identifier and term expression in the right order
        /// </summary>
        /// <param name="left"> The left. </param>
        /// <param name="right"> The right. </param>
        /// <param name="compareOp"> The compare operation. </param>
        /// <param name="compareOpSwitched"> The compare operation switched. </param>
        /// <returns> true expression if the relation is succesfully created </returns>
        /// <exception cref="CqlLinqException">Can't determine the (column) identfier for the CQL where relation
        ///   or
        ///   Error creating relation. Not able to detect the correct term to create the relation with.</exception>
        private Expression CreateRelation(Expression left, Expression right, CqlExpressionType compareOp,
                                          CqlExpressionType compareOpSwitched)
        {
            bool shuffled = false;
            if (left.GetType() != typeof(SelectorExpression))
            {
                if (right.GetType() != typeof(SelectorExpression))
                    throw new CqlLinqException("Can't determine the column/token selector for the CQL where relation");

                //swap expressions
                Expression temp = left;
                left = right;
                right = temp;

                shuffled = true;
            }

            if (right.GetType() != typeof(TermExpression))
                throw new CqlLinqException("Could not detect term in the CQL where relation");

            _relations.Add(new RelationExpression((SelectorExpression)left, shuffled ? compareOpSwitched : compareOp,
                                                  (TermExpression)right));

            return Expression.Constant(true);
        }

        /// <summary>
        ///   Replaces constants with Cql Terms
        /// </summary>
        /// <param name="node"> The expression to visit. </param>
        /// <returns> The modified expression, if it or any subexpression was modified; otherwise, returns the original expression. </returns>
        protected override Expression VisitConstant(ConstantExpression node)
        {
            //check if it is a valid CQL type
            CqlType.CreateType(node.Type);
            
            //check if it is a map
            if (node.Type.Implements(typeof(IDictionary<,>)))
            {
                var terms = new Dictionary<TermExpression, TermExpression>();
                foreach (DictionaryEntry elem in (IDictionary)node.Value)
                    terms.Add(new TermExpression(elem.Key), new TermExpression(elem.Value));

                return new TermExpression(terms);
            }

            //check if it is a set
            if (node.Type.Implements(typeof(ISet<>)))
            {
                var terms = new HashSet<TermExpression>();
                foreach (var elem in (IEnumerable)node.Value)
                    terms.Add(new TermExpression(elem));

                return new TermExpression(terms);
            }

            //check if it is a collection (and therefore will be represented as List)
            if (node.Type != typeof(string) && node.Type != typeof(byte[]) &&
                node.Type.Implements(typeof(IEnumerable<>)))
            {
                var terms = new List<TermExpression>();
                foreach (var elem in (IEnumerable)node.Value)
                    terms.Add(new TermExpression(elem));

                return new TermExpression(terms);
            }

            return new TermExpression(node.Value);
        }

        /// <summary>
        ///   Performs simple type checking to see if new expression will result in valid CQL type
        /// </summary>
        /// <param name="node"> The expression to visit. </param>
        /// <returns> The modified expression, if it or any subexpression was modified; otherwise, returns the original expression. </returns>
        /// <exception cref="CqlLinqException"></exception>
        protected override Expression VisitNew(NewExpression node)
        {
            CqlType.CreateType(node.Type);
            
            return base.VisitNew(node);
        }
    }
}