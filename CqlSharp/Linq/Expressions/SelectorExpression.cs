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
    ///   A CQL Selector (element in select clause)
    /// </summary>
    internal class SelectorExpression : Expression
    {
        private readonly ReadOnlyCollection<SelectorExpression> _arguments;
        private readonly Functions _function;
        private readonly IdentifierExpression _identifier;
        private readonly CqlExpressionType _selectorType;
        private readonly Type _type;

        public SelectorExpression(IdentifierExpression identifier)
        {
            if (identifier == null) throw new ArgumentNullException("identifier");

            _identifier = identifier;
            _selectorType = CqlExpressionType.IdentifierSelector;
            _type = identifier.Type;
        }

        public SelectorExpression(CqlExpressionType writeTimeOrTtl, IdentifierExpression identifier)
        {
            switch (writeTimeOrTtl)
            {
                case CqlExpressionType.TtlSelector:
                    _type = typeof(int);
                    break;
                case CqlExpressionType.WriteTimeSelector:
                    _type = typeof(DateTime);
                    break;
                default:
                    throw new ArgumentException("ExpressionType must be TTL IdentifierSelector or WriteTime selector");
            }

            _selectorType = writeTimeOrTtl;
            _identifier = identifier;
        }

        public SelectorExpression(Functions function, IList<SelectorExpression> arguments)
        {
            if (arguments == null) throw new ArgumentNullException("arguments");

            switch (function)
            {
                case Functions.UnixTimestampOf:
                    _type = typeof(long);
                    break;
                case Functions.DateOf:
                case Functions.Now:
                    _type = typeof(DateTime);
                    break;
                case Functions.MinTimeUuid:
                case Functions.MaxTimeUuid:
                    _type = typeof(Guid);
                    break;
                case Functions.Token:
                    _type = typeof(object); //depends on used partitioner
                    break;
            }
            _function = function;
            _arguments = new ReadOnlyCollection<SelectorExpression>(arguments);
        }

        private SelectorExpression(Type type, CqlExpressionType selectorType, IdentifierExpression identifier, Functions function, IList<SelectorExpression> arguments)
        {
            _type = type;
            _selectorType = selectorType;
            _identifier = identifier;
            _function = function;
            _arguments = new ReadOnlyCollection<SelectorExpression>(arguments);
        }


        public override Type Type
        {
            get { return _type; }
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)_selectorType; }
        }

        public IdentifierExpression Identifier
        {
            get { return _identifier; }
        }

        public Functions Function
        {
            get { return _function; }
        }

        public ReadOnlyCollection<SelectorExpression> Arguments
        {
            get { return _arguments; }
        }

        protected override Expression Accept(ExpressionVisitor visitor)
        {
            var type = visitor as CqlExpressionVisitor;

            if (type != null)
            {
                return type.VisitSelector(this);
            }

            return base.Accept(visitor);
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            bool changed = false;

            IdentifierExpression identifier = null;
            if (_identifier != null)
            {
                identifier = (IdentifierExpression)visitor.Visit(_identifier);
                changed |= identifier != _identifier;
            }

            SelectorExpression[] args = null;
            if (_arguments != null)
            {
                int count = _arguments.Count;
                args = new SelectorExpression[count];
                for (int i = 0; i < count; i++)
                {
                    args[i] = (SelectorExpression)visitor.Visit(_arguments[i]);
                    changed |= args[i] != _arguments[i];
                }
            }

            return changed ? new SelectorExpression(_type, _selectorType, identifier, _function, args) : this;
        }
    }
}