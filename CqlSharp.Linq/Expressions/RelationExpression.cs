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

using System;
using System.Linq.Expressions;

namespace CqlSharp.Linq.Expressions
{
    /// <summary>
    ///   A CQL Relation (a single comparison statement in a where-clause)
    /// </summary>
    internal class RelationExpression : Expression
    {
        private readonly CqlExpressionType _relation;
        private readonly SelectorExpression _selector;
        private readonly TermExpression _term;

        /// <summary>
        ///   Initializes a new instance of the <see cref="RelationExpression" /> class.
        /// </summary>
        /// <param name="selector"> The selector. </param>
        /// <param name="relation"> The relation. </param>
        /// <param name="term"> The term. </param>
        /// <exception cref="System.ArgumentNullException">selector
        ///   or
        ///   term</exception>
        public RelationExpression(SelectorExpression selector, CqlExpressionType relation, TermExpression term)
        {
            if (selector == null) throw new ArgumentNullException("selector");

            if (term == null)
                throw new ArgumentNullException("term");

            _selector = selector;
            _relation = relation;
            _term = term;
        }


        public override ExpressionType NodeType
        {
            get { return (ExpressionType) _relation; }
        }

        public override Type Type
        {
            get { return typeof (bool); }
        }

        public TermExpression Term
        {
            get { return _term; }
        }

        public SelectorExpression Selector
        {
            get { return _selector; }
        }

        protected override Expression Accept(ExpressionVisitor visitor)
        {
            var type = visitor as CqlExpressionVisitor;

            if (type != null)
            {
                return type.VisitRelation(this);
            }

            return base.Accept(visitor);
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var selector = (SelectorExpression) visitor.Visit(_selector);
            var term = (TermExpression) visitor.Visit(_term);

            if (selector != _selector || term != _term)
                return new RelationExpression(selector, _relation, term);

            return this;
        }
    }
}