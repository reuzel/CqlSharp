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
using System.Linq.Expressions;

namespace CqlSharp.Linq.Expressions
{
    /// <summary>
    ///   Specifies an ordering (OrderBy element)
    /// </summary>
    internal class OrderingExpression : Expression
    {
        private readonly IdentifierExpression _identifier;
        private readonly CqlExpressionType _order;
        private readonly Type _type;

        public OrderingExpression(Type type, IdentifierExpression identifier, CqlExpressionType orderType)
        {
            if (type == null)
                throw new ArgumentNullException("type");

            if (identifier == null)
                throw new ArgumentNullException("identifier");

            if (orderType != CqlExpressionType.OrderAscending || orderType != CqlExpressionType.OrderDescending)
                throw new ArgumentException("ExpressionType must be OrderAscending or OrderDescending", "orderType");

            _type = type;
            _identifier = identifier;
            _order = orderType;
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)_order; }
        }

        public override Type Type
        {
            get { return _type; }
        }

        public IdentifierExpression Identifier
        {
            get { return _identifier; }
        }

        protected override Expression Accept(ExpressionVisitor visitor)
        {
            var type = visitor as CqlExpressionVisitor;

            if (type != null)
            {
                return type.VisitOrdering(this);
            }

            return base.Accept(visitor);
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var identifier = visitor.Visit(_identifier);

            return identifier != _identifier ? identifier : _identifier;
        }
    }
}