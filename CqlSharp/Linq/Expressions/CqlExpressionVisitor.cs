// CqlSharp - CqlSharp
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

using System.Linq.Expressions;

namespace CqlSharp.Linq.Expressions
{
    /// <summary>
    ///   Extends ExpressionVisitor with Visit methods specific for CQL expressions
    /// </summary>
    internal class CqlExpressionVisitor : ExpressionVisitor
    {
        public virtual Expression VisitProjection(ProjectionExpression node)
        {
            return base.VisitExtension(node);
        }

        public virtual Expression VisitSelectStatement(SelectStatementExpression selectStatement)
        {
            return base.VisitExtension(selectStatement);
        }

        public virtual Expression VisitSelector(SelectorExpression node)
        {
            return base.VisitExtension(node);
        }

        public virtual Expression VisitOrdering(OrderingExpression node)
        {
            return base.VisitExtension(node);
        }

        /// <summary>
        ///   Visits the relation in a where clause.
        /// </summary>
        /// <param name="node"> The node. </param>
        /// <returns> </returns>
        public virtual Expression VisitRelation(RelationExpression node)
        {
            return base.VisitExtension(node);
        }

        /// <summary>
        ///   Visits the CQL term.
        /// </summary>
        /// <param name="node"> The node. </param>
        /// <returns> </returns>
        public virtual Expression VisitTerm(TermExpression node)
        {
            return base.VisitExtension(node);
        }

        /// <summary>
        ///   Visits the identifier.
        /// </summary>
        /// <param name="identifier"> The node. </param>
        /// <returns> </returns>
        public virtual Expression VisitIdentifier(IdentifierExpression identifier)
        {
            return base.VisitExtension(identifier);
        }

        /// <summary>
        ///   Visits the select clause.
        /// </summary>
        /// <param name="selectClauseExpression"> The select clause expression. </param>
        /// <returns> </returns>
        public virtual Expression VisitSelectClause(SelectClauseExpression selectClauseExpression)
        {
            return base.VisitExtension(selectClauseExpression);
        }
    }
}