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
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace CqlSharp.Linq.Expressions
{
    /// <summary>
    ///   Represents a CQL terms (value)
    /// </summary>
    internal class TermExpression : Expression
    {
        private readonly CqlExpressionType _termType;
        private readonly Type _type;

        //used for variable terms ('?')
        private readonly int _order;

        //values in case of a dictionary term
        private readonly ReadOnlyDictionary<TermExpression, TermExpression> _dictionaryTerms;

        //values for set/list terms and for function arguments
        private readonly ReadOnlyCollection<TermExpression> _terms;

        //invoked function
        private readonly MethodInfo _function;

        //value for scalar values
        private readonly object _value;

        public TermExpression(Type parameterType, int order)
        {
            _order = order;
            _type = parameterType;
            _termType = CqlExpressionType.Variable;
        }

        public TermExpression(Object value)
        {
            if (value == null) throw new ArgumentNullException("value");

            ////check if type is a supported CQL type
            // if (!value.GetType().IsSupportedCqlType())
            //    throw new CqlLinqException(string.Format("Type {0} can't be coverted to a CQL value", value.GetType()));

            _type = value.GetType();
            _termType = CqlExpressionType.Constant;
            _value = value;
        }

        public TermExpression(IList<TermExpression> terms)
        {
            if (terms == null || terms.Count == 0)
                throw new CqlLinqException("Empty lists are not allowed");

            _type = typeof(IList<>).MakeGenericType(terms[0].Type);
            _terms = terms.AsReadOnly();
            _termType = CqlExpressionType.List;
        }

        public TermExpression(ISet<TermExpression> terms)
        {
            if (terms == null || terms.Count == 0)
                throw new CqlLinqException("Empty lists are not allowed");

            _type = typeof(ISet<>).MakeGenericType(terms.First().Type);
            _terms = terms.ToList().AsReadOnly();
            _termType = CqlExpressionType.Set;
        }

        public TermExpression(IDictionary<TermExpression, TermExpression> terms)
        {
            if (terms == null || terms.Count == 0)
                throw new CqlLinqException("Empty dictionaries are not allowed");

            var firstElement = terms.First();
            _type = typeof(IDictionary<,>).MakeGenericType(firstElement.Key.Type, firstElement.Value.Type);
            _dictionaryTerms = terms.AsReadOnly();
            _termType = CqlExpressionType.Map;
        }

        public TermExpression(MethodInfo function, IEnumerable<TermExpression> arguments)
        {
            if (arguments == null)
                throw new ArgumentNullException("arguments");

            _function = function;
            _terms = arguments.AsReadOnly();
            _termType = CqlExpressionType.Function;
            _type = function.ReturnType;
        }

        private TermExpression(TermExpression original, IEnumerable<TermExpression> terms,
                               IDictionary<TermExpression, TermExpression> dictTerms)
        {
            _function = original.Function;
            _termType = original._termType;
            _type = original.Type;
            _value = original.Value;
            _terms = terms.AsReadOnly();
            _dictionaryTerms = dictTerms.AsReadOnly();
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

        public MethodInfo Function
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

        public int Order
        {
            get { return _order; }
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
            bool changedTerms;
            var terms = _terms.VisitAll(visitor, out changedTerms);

            bool changedDictTerms;
            var dictTerms = _dictionaryTerms.VisitAll(visitor, out changedDictTerms);

            if (changedTerms || changedDictTerms)
                return new TermExpression(this, terms, dictTerms);

            return this;
        }
    }
}