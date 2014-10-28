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

using CqlSharp.Linq.Expressions;
using System;
using System.Linq.Expressions;
using System.Reflection;

namespace CqlSharp.Linq.Query
{
    /// <summary>
    ///   Converts an expression with selector expressions to a lambda expression that takes a datareader as input.
    /// </summary>
    internal class ProjectorBuilder : CqlExpressionVisitor
    {
        private static readonly ConstructorInfo TokenConstructor =
            typeof(CqlToken).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, null,
                                             CallingConventions.HasThis,
                                             new[] { typeof(object) }, null);

        private static readonly PropertyInfo Indexer = typeof(CqlDataReader).GetProperty("Item",
                                                                                          new[] { typeof(int) });

        private ParameterExpression _reader;
        private ParameterExpression _arguments;

        public LambdaExpression BuildProjector(Expression expression)
        {
            _reader = Expression.Parameter(typeof(CqlDataReader), "cqlDataReader");
            _arguments = Expression.Parameter(typeof(object[]), "arguments");

            Expression expr = Visit(expression);
            return Expression.Lambda(expr, _reader, _arguments);
        }

        /// <summary>
        /// replaces selectors (column references) expressions, with an expression that reads the corresponding
        /// value from DataReader
        /// </summary>
        /// <param name="selector"></param>
        /// <returns></returns>
        public override Expression VisitSelector(SelectorExpression selector)
        {
            Expression value;

            //check if it is a token (of which we don't know it's type)
            if (selector.Type == typeof(CqlToken))
                return Expression.New(TokenConstructor,
                                      Expression.MakeIndex(_reader, Indexer, new[] { Expression.Constant(selector.Ordinal) }));

            //normal case
            value = Expression.Call(_reader, "GetFieldValue", new [] { selector.Type }, Expression.Constant(selector.Ordinal));

            
            //check for null values in case of a Nullable type
            if (selector.Type.IsGenericType && selector.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                value = Expression.Condition(
                    Expression.Call(_reader, "IsDBNull", null, Expression.Constant(selector.Ordinal)),
                    Expression.Constant(null, selector.Type),
                    Expression.Convert(value, selector.Type)
                    );
            }

            return value;
        }

        /// <summary>
        /// Replaces Variable terms with references to the correct argument
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
        public override Expression VisitTerm(TermExpression node)
        {
            if (node.NodeType != (ExpressionType)CqlExpressionType.Variable)
                throw new CqlLinqException("Unexpected type of term in a select clause: " + ((CqlExpressionType)node.NodeType).ToString());

            var argument = Expression.ArrayIndex(_arguments, Expression.Constant(node.Order));
            return Expression.Convert(argument, node.Type);
        }
    }
}