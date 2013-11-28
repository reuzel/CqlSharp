// CqlSharp - CqlSharp
// Copyright (c) 2013 Joost Reuzel
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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace CqlSharp.Linq.Expressions
{
    /// <summary>
    ///   A CQL Relation (a single comparison statement in a where-clause)
    /// </summary>
    internal class RelationExpression : Expression
    {
        private readonly ReadOnlyCollection<IdentifierExpression> _identifiers;
        private readonly CqlExpressionType _relation;
        private readonly ReadOnlyCollection<TermExpression> _terms;

        /// <summary>
        /// Initializes a new instance of the <see cref="RelationExpression"/> class.
        /// Used for any non token related relation
        /// </summary>
        /// <param name="identifier">The identifier.</param>
        /// <param name="relation">The relation.</param>
        /// <param name="term">The term.</param>
        /// <exception cref="System.ArgumentNullException">
        /// identifier
        /// or
        /// term
        /// </exception>
        /// <exception cref="System.ArgumentException">The provided ExpressionType is not a valid relation</exception>
        public RelationExpression(IdentifierExpression identifier, CqlExpressionType relation, TermExpression term)
        {

            if (identifier == null)
                throw new ArgumentNullException("identifier");

            if (term == null)
                throw new ArgumentNullException("term");

            _identifiers = new ReadOnlyCollection<IdentifierExpression>(new[] { identifier });
            _relation = relation;
            _terms = new ReadOnlyCollection<TermExpression>(new[] { term });
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RelationExpression"/> class.
        /// Used for an token comparison relation
        /// </summary>
        /// <param name="identifiers">The identifiers.</param>
        /// <param name="relation">The relation.</param>
        /// <param name="term">The term.</param>
        /// <exception cref="System.ArgumentNullException">
        /// identifiers
        /// or
        /// term
        /// </exception>
        /// <exception cref="System.ArgumentException">
        /// The provided ExpressionType is not a valid relation
        /// or
        /// The provided ExpressionType is not a valid (token) relation
        /// </exception>
        public RelationExpression(IList<IdentifierExpression> identifiers, CqlExpressionType relation, TermExpression term)
        {
            if (identifiers == null)
                throw new ArgumentNullException("identifiers");

            if (!relation.ToString().Contains("Relation"))
                throw new ArgumentException("The provided ExpressionType is not a valid relation");

            if (!relation.ToString().Contains("Token"))
                throw new ArgumentException("The provided ExpressionType is not a valid (token) relation");

            if (term == null)
                throw new ArgumentNullException("term");

            _identifiers = identifiers.AsReadOnly();
            _relation = relation;
            _terms = new ReadOnlyCollection<TermExpression>(new[] { term });
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RelationExpression"/> class. 
        /// Used for an IN relation
        /// </summary>
        /// <param name="identifier">The identifier.</param>
        /// <param name="terms">The term.</param>
        /// <exception cref="System.ArgumentNullException">
        /// identifier
        /// or
        /// term
        /// </exception>
        public RelationExpression(IdentifierExpression identifier, IList<TermExpression> terms)
        {
            if (identifier == null)
                throw new ArgumentNullException("identifier");

            if (terms == null)
                throw new ArgumentNullException("terms");

            _identifiers = new ReadOnlyCollection<IdentifierExpression>(new[] { identifier });
            _relation = CqlExpressionType.In;
            _terms = terms.AsReadOnly();
        }

        private RelationExpression(IList<IdentifierExpression> identifiers, CqlExpressionType relation, IList<TermExpression> terms)
        {
            _identifiers = identifiers.AsReadOnly();
            _relation = relation;
            _terms = terms.AsReadOnly();
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)_relation; }
        }

        public override Type Type
        {
            get { return typeof(bool); }
        }

        public ReadOnlyCollection<IdentifierExpression> Identifiers
        {
            get { return _identifiers; }
        }

        public ReadOnlyCollection<TermExpression> Terms
        {
            get { return _terms; }
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
            bool changed = false;

            var terms = new List<TermExpression>();
            foreach (var term in _terms)
            {
                var visitedTerm = (TermExpression)visitor.Visit(term);
                terms.Add(visitedTerm);
                changed |= term != visitedTerm;
            }

            var identifiers = new List<IdentifierExpression>();
            foreach (var id in _identifiers)
            {
                var visitedId = (IdentifierExpression)visitor.Visit(id);
                identifiers.Add(visitedId);
                changed |= !visitedId.Equals(id);
            }

            if (changed)
                return new RelationExpression(identifiers, _relation, terms);

            return this;
        }
    }
}