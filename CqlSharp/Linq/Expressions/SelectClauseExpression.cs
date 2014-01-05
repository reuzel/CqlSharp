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

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace CqlSharp.Linq.Expressions
{
    /// <summary>
    ///   A select clause as part of a select query
    /// </summary>
    internal class SelectClauseExpression : Expression
    {
        private readonly bool? _distinct;
        private readonly CqlExpressionType _nodeType;
        private readonly ReadOnlyCollection<SelectorExpression> _selectors;

        public SelectClauseExpression(bool count)
        {
            _nodeType = count ? CqlExpressionType.SelectCount : CqlExpressionType.SelectAll;
        }

        public SelectClauseExpression(IList<SelectorExpression> selectors, bool? distinct = null)
        {
            _nodeType = CqlExpressionType.SelectColumns;
            _distinct = distinct;
            _selectors = selectors.AsReadOnly();
        }

        public bool? Distinct
        {
            get { return _distinct; }
        }

        public ReadOnlyCollection<SelectorExpression> Selectors
        {
            get { return _selectors; }
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType) _nodeType; }
        }

        public override Type Type
        {
            get
            {
                switch (_nodeType)
                {
                    case CqlExpressionType.SelectColumns:
                        return typeof (IEnumerable<object>);
                    case CqlExpressionType.SelectCount:
                        return typeof (int);
                    default:
                        throw new CqlLinqException("Unexpected SekectClayse ExpressionType: " + _nodeType);
                }
            }
        }

        protected override Expression Accept(ExpressionVisitor visitor)
        {
            var type = visitor as CqlExpressionVisitor;

            if (type != null)
            {
                return type.VisitSelectClause(this);
            }

            return base.Accept(visitor);
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            if (_selectors != null)
            {
                bool changed = false;
                int count = _selectors.Count;
                var selectors = new SelectorExpression[count];
                for (int i = 0; i < count; i++)
                {
                    selectors[i] = (SelectorExpression) visitor.Visit(_selectors[i]);
                    changed |= selectors[i] != _selectors[i];
                }

                if (changed)
                {
                    return new SelectClauseExpression(selectors, _distinct);
                }
            }

            return this;
        }
    }
}