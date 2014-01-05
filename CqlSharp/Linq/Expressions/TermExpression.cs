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
using System.Linq;
using System.Linq.Expressions;

namespace CqlSharp.Linq.Expressions
{
    /// <summary>
    ///   Represents a CQL terms (value)
    /// </summary>
    internal class TermExpression : Expression
    {
        private readonly ReadOnlyDictionary<TermExpression, TermExpression> _dictionaryTerms;
        private readonly Functions _function;
        private readonly CqlExpressionType _termType;
        private readonly ReadOnlyCollection<TermExpression> _terms;
        private readonly Type _type;
        private readonly object _value;

        public TermExpression(Object value)
        {
            if (value == null) throw new ArgumentNullException("value");

            //check if type is a supported CQL type
            if (!value.GetType().IsCqlType())
                throw new CqlLinqException(string.Format("Type {0} can't be coverted to a CQL value", value.GetType()));

            _type = value.GetType();
            _termType = CqlExpressionType.Constant;
            _value = value;
        }

        public TermExpression(Type type, CqlExpressionType termType, IList<TermExpression> terms)
        {
            if (type == null) throw new ArgumentNullException("type");

            if (termType != CqlExpressionType.List && termType != CqlExpressionType.Set)
                throw new ArgumentException("Type of term must be Set or List");

            _type = type;
            _terms = terms.AsReadOnly();
            _termType = termType;
        }

        public TermExpression(Type type, IDictionary<TermExpression, TermExpression> terms)
        {
            if (type == null) throw new ArgumentNullException("type");

            _type = type;
            _dictionaryTerms = terms.AsReadOnly();
            _termType = CqlExpressionType.Map;
        }

        public TermExpression(Functions function, IList<TermExpression> arguments)
        {
            if (arguments == null)
                throw new ArgumentNullException("arguments");

            _terms = new ReadOnlyCollection<TermExpression>(arguments);
            _function = function;
            _termType = CqlExpressionType.Function;
            switch (function)
            {
                case Functions.Token:
                    _type = typeof (object); //depends on partitioner
                    break;
                case Functions.DateOf:
                    _type = typeof (DateTime);
                    if (_terms.Single().Type != typeof (Guid))
                        throw new ArgumentException("terms should consist of a single GUID", "arguments");
                    break;

                case Functions.MinTimeUuid:
                case Functions.MaxTimeUuid:
                    _type = typeof (Guid);
                    if (_terms.Single().Type != typeof (DateTime))
                        throw new ArgumentException("terms should consist of a single DateTime", "arguments");
                    break;

                case Functions.UnixTimestampOf:
                    _type = typeof (long);
                    if (_terms.Single().Type != typeof (Guid))
                        throw new ArgumentException("terms should consist of a single GUID", "arguments");
                    break;

                case Functions.Now:
                    _type = typeof (Guid);
                    if (_terms.Count() != 0)
                        throw new ArgumentException("terms should not contain any value", "arguments");

                    break;

                default:
                    throw new ArgumentException("Unknown function!", "function");
            }
        }

        private TermExpression(Type type, Functions function, IList<TermExpression> terms,
                               IDictionary<TermExpression, TermExpression> dictionary)
        {
            if (type == null) throw new ArgumentNullException("type");
            if (terms == null && dictionary == null)
                throw new ArgumentException("Either terms or dictionary must be set");

            _type = type;
            _function = function;

            if (terms != null)
                _terms = terms.AsReadOnly();
            else
                _dictionaryTerms = dictionary.AsReadOnly();
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType) _termType; }
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

        public ReadOnlyCollection<TermExpression> Terms
        {
            get { return _terms; }
        }

        public ReadOnlyDictionary<TermExpression, TermExpression> DictionaryTerms
        {
            get { return _dictionaryTerms; }
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
            if (_terms != null)
            {
                bool changed = false;

                TermExpression[] terms = null;
                if (_terms != null)
                {
                    int count = _terms.Count;
                    terms = new TermExpression[count];
                    for (int i = 0; i < count; i++)
                    {
                        terms[i] = (TermExpression) visitor.Visit(_terms[i]);
                        changed |= terms[i] != _terms[i];
                    }
                }

                Dictionary<TermExpression, TermExpression> dictionaryTerms = null;
                if (_dictionaryTerms != null)
                {
                    dictionaryTerms = new Dictionary<TermExpression, TermExpression>();
                    foreach (var pair in _dictionaryTerms)
                    {
                        var key = (TermExpression) visitor.Visit(pair.Key);
                        var value = (TermExpression) visitor.Visit(pair.Value);
                        changed |= (pair.Key != key) || (pair.Value != value);
                        dictionaryTerms.Add(key, value);
                    }
                }

                if (changed)
                    return new TermExpression(_type, _function, terms, dictionaryTerms);
            }

            return this;
        }
    }
}