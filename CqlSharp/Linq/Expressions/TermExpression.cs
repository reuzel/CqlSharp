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
using System.Linq;
using System.Linq.Expressions;

namespace CqlSharp.Linq.Expressions
{
    /// <summary>
    ///   Represents a CQL terms (value)
    /// </summary>
    internal class TermExpression : Expression
    {
        private readonly ReadOnlyCollection<TermExpression> _arguments;
        private readonly Functions _function;
        private readonly CqlExpressionType _termType;
        private readonly Type _type;
        private readonly object _value;

        public TermExpression(CqlExpressionType termType, Object value)
        {
            if (value == null) throw new ArgumentNullException("value");

            switch (termType)
            {
                case CqlExpressionType.Set:
                case CqlExpressionType.List:
                    _type = TypeSystem.FindIEnumerable(value.GetType());
                    if (_type == null)
                        throw new ArgumentException("value must implement IEnumerable<>", "value");
                    break;

                case CqlExpressionType.Map:
                    var type = TypeSystem.FindIEnumerable(value.GetType());
                    if (!type.IsGenericType || type.GetGenericTypeDefinition() != typeof(KeyValuePair<,>))
                        throw new ArgumentException("value must implement IDictionary<>", "value");
                    _type = typeof(IDictionary<,>).MakeGenericType(type.GetGenericArguments());
                    break;

                case CqlExpressionType.Constant:
                    _type = value.GetType();
                    break;

                default:
                    throw new ArgumentException("termType must be reflect a constant, set, list or map");
            }

            _termType = termType;
            _value = value;
        }

        public TermExpression(Functions function, IList<TermExpression> arguments)
        {
            if (arguments == null)
                throw new ArgumentNullException("arguments");

            _arguments = new ReadOnlyCollection<TermExpression>(arguments);
            _function = function;
            _termType = CqlExpressionType.Function;
            switch (function)
            {
                case Functions.Token:
                    _type = typeof(object); //depends on partitioner
                    break;
                case Functions.DateOf:
                    _type = typeof(DateTime);
                    if (_arguments.Single().Type != typeof(Guid))
                        throw new ArgumentException("arguments should consist of a single GUID", "arguments");
                    break;

                case Functions.MinTimeUuid:
                case Functions.MaxTimeUuid:
                    _type = typeof(Guid);
                    if (_arguments.Single().Type != typeof(DateTime))
                        throw new ArgumentException("arguments should consist of a single DateTime", "arguments");
                    break;

                case Functions.UnixTimestampOf:
                    _type = typeof(long);
                    if (_arguments.Single().Type != typeof(Guid))
                        throw new ArgumentException("arguments should consist of a single GUID", "arguments");
                    break;

                case Functions.Now:
                    _type = typeof(Guid);
                    if (_arguments.Count() != 0)
                        throw new ArgumentException("arguments should not contain any value", "arguments");

                    break;

                default:
                    throw new ArgumentException("Unknown function!", "function");
            }
        }

        private TermExpression(Type type, Functions function, IList<TermExpression> arguments)
        {
            if (type == null) throw new ArgumentNullException("type");
            if (arguments == null) throw new ArgumentNullException("arguments");

            _type = type;
            _function = function;
            _arguments = new ReadOnlyCollection<TermExpression>(arguments);
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)_termType; }
        }

        public object Value
        {
            get { return _value; }
        }

        public override Type Type
        {
            get { return _type; }
        }

        public Functions Function
        {
            get { return _function; }
        }

        public ReadOnlyCollection<TermExpression> Arguments
        {
            get { return _arguments; }
        }
        
        protected override Expression Accept(ExpressionVisitor visitor)
        {
            var type = visitor as CqlExpressionVisitor;

            if (type != null)
            {
                return type.VisitTerm(this);
            }

            return base.Accept(visitor);
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            if (_arguments != null)
            {
                bool changed = false;

                int count = _arguments.Count;
                var args = new TermExpression[count];
                for (int i = 0; i < count; i++)
                {
                    args[i] = (TermExpression)visitor.Visit(_arguments[i]);
                    changed |= args[i] != _arguments[i];
                }

                if (changed)
                    return new TermExpression(_type, _function, args);
            }

            return this;
        }
    }
}