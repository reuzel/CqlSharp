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
        private readonly ReadOnlyCollection<Expression> _identifiers;
        private readonly CqlExpressionType _relation;
        private readonly ReadOnlyCollection<Expression> _terms;

        /// <summary>
        /// Initializes a new instance of the <see cref="RelationExpression"/> class.
        /// Used for any non token related relation
        /// </summary>
        /// <param name="identifier">The identifier.</param>
        /// <param name="relation">The relation.</param>
        /// <param name="terms">The terms.</param>
        /// <exception cref="System.ArgumentNullException">
        /// identifier
        /// or
        /// terms
        /// </exception>
        /// <exception cref="System.ArgumentException">The provided ExpressionType is not a valid relation</exception>
        public RelationExpression(Expression identifier, CqlExpressionType relation, Expression terms)
        {
            if (identifier == null)
                throw new ArgumentNullException("identifier");

            if (!relation.ToString().Contains("Relation"))
                throw new ArgumentException("The provided ExpressionType is not a valid relation");

            if (terms == null)
                throw new ArgumentNullException("terms");

            _identifiers = new ReadOnlyCollection<Expression>(new[] { identifier });
            _relation = relation;
            _terms = new ReadOnlyCollection<Expression>(new[] { terms });
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RelationExpression"/> class.
        /// Used for an token comparison relation
        /// </summary>
        /// <param name="identifiers">The identifiers.</param>
        /// <param name="relation">The relation.</param>
        /// <param name="terms">The terms.</param>
        /// <exception cref="System.ArgumentNullException">
        /// identifiers
        /// or
        /// terms
        /// </exception>
        /// <exception cref="System.ArgumentException">
        /// The provided ExpressionType is not a valid relation
        /// or
        /// The provided ExpressionType is not a valid (token) relation
        /// </exception>
        public RelationExpression(IList<Expression> identifiers, CqlExpressionType relation, Expression terms)
        {
            if (identifiers == null)
                throw new ArgumentNullException("identifiers");

            if (!relation.ToString().Contains("Relation"))
                throw new ArgumentException("The provided ExpressionType is not a valid relation");

            if (!relation.ToString().Contains("Token"))
                throw new ArgumentException("The provided ExpressionType is not a valid (token) relation");

            if (terms == null)
                throw new ArgumentNullException("terms");

            _identifiers = new ReadOnlyCollection<Expression>(identifiers);
            _relation = relation;
            _terms = new ReadOnlyCollection<Expression>(new[] { terms });
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RelationExpression"/> class. 
        /// Used for an IN relation
        /// </summary>
        /// <param name="identifier">The identifier.</param>
        /// <param name="terms">The terms.</param>
        /// <exception cref="System.ArgumentNullException">
        /// identifier
        /// or
        /// terms
        /// </exception>
        public RelationExpression(Expression identifier, IList<Expression> terms)
        {
            if (identifier == null)
                throw new ArgumentNullException("identifier");

            if (terms == null)
                throw new ArgumentNullException("terms");

            _identifiers = new ReadOnlyCollection<Expression>(new[] { identifier });
            _relation = CqlExpressionType.In;
            _terms = new ReadOnlyCollection<Expression>(terms);
        }

        private RelationExpression(IList<Expression> identifiers, CqlExpressionType relation, IList<Expression> terms)
        {
            _identifiers = new ReadOnlyCollection<Expression>(identifiers);
            _relation = relation;
            _terms = new ReadOnlyCollection<Expression>(terms);
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)_relation; }
        }

        public override Type Type
        {
            get { return typeof(bool); }
        }

        public ReadOnlyCollection<Expression> Identifiers
        {
            get { return _identifiers; }
        }

        public ReadOnlyCollection<Expression> Terms
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

            var terms = new List<Expression>();
            foreach (var term in _terms)
            {
                var visitedTerm = visitor.Visit(term);
                terms.Add(visitedTerm);
                changed |= term != visitedTerm;
            }

            var identifiers = new List<Expression>();
            foreach (var id in _identifiers)
            {
                var visitedId = visitor.Visit(id);
                identifiers.Add(visitedId);
                changed |= visitedId != id;
            }

            if (changed)
                return new RelationExpression(identifiers, _relation, terms);

            return this;
        }
    }
}