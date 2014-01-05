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

using System.Linq.Expressions;
using System.Reflection;
using CqlSharp.Linq.Expressions;

namespace CqlSharp.Linq
{
    /// <summary>
    ///   Converts an expression with identifier expressions to a lambda expression that takes a datareader as input.
    /// </summary>
    internal class ProjectorBuilder : CqlExpressionVisitor
    {
        private static readonly PropertyInfo Indexer = typeof (CqlDataReader).GetProperty("Item",
                                                                                          new[] {typeof (string)});

        private ParameterExpression _reader;

        public LambdaExpression BuildProjector(Expression expression)
        {
            _reader = Expression.Parameter(typeof (CqlDataReader), "cqlDataReader");
            Expression expr = Visit(expression);
            return Expression.Lambda(expr, _reader);
        }

        public override Expression VisitIdentifier(IdentifierExpression identifier)
        {
            var value = Expression.MakeIndex(_reader, Indexer, new[] {Expression.Constant(identifier.Name)});

            if (identifier.Type.IsValueType)
            {
                return Expression.Condition(
                    Expression.Equal(value, Expression.Constant(null)),
                    Expression.Default(identifier.Type),
                    Expression.Convert(value, identifier.Type));
            }

            return Expression.Convert(value, identifier.Type);
        }
    }
}