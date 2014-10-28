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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Reflection;

namespace CqlSharp.Linq.Expressions
{
    /// <summary>
    ///   A CQL Selector (element in select clause)
    /// </summary>
    internal class SelectorExpression : Expression
    {
        private readonly ReadOnlyCollection<SelectorExpression> _arguments;
        private readonly MethodInfo _function;
        private readonly string _identifier;
        private readonly CqlExpressionType _selectorType;
        private readonly Type _type;

        public SelectorExpression(string identifier, Type type)
        {
            if (identifier == null) throw new ArgumentNullException("identifier");

            _identifier = identifier;
            _selectorType = CqlExpressionType.IdentifierSelector;
            _type = type;
        }

        public SelectorExpression(MethodInfo function, IEnumerable<SelectorExpression> arguments)
        {
            if (function.DeclaringType != typeof (CqlFunctions))
                throw new ArgumentException("function must be a valid Cql Function");

            _function = function;
            _arguments = arguments.AsReadOnly();
            _type = function.ReturnType;
            _selectorType = CqlExpressionType.FunctionSelector;
        }

        public int Ordinal { get; set; }

        public override Type Type
        {
            get { return _type; }
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType) _selectorType; }
        }

        public string Identifier
        {
            get { return _identifier; }
        }

        public MethodInfo Function
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
            if (_arguments != null)
            {
                bool changed = false;
                var args = _arguments.VisitAll(visitor, out changed);

                if (changed)
                    return new SelectorExpression(_function, args);
            }

            return this;
        }
    }
}