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
    ///   A CQL identifier
    /// </summary>
    internal class IdentifierExpression : Expression
    {
        private readonly string _name;
        private readonly Type _type;

        public IdentifierExpression(Type type, string name)
        {
            if (type == null) throw new ArgumentNullException("type");
            if (name == null) throw new ArgumentNullException("name");
            _type = type;
            _name = name;
        }

        public override Type Type
        {
            get { return _type; }
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)CqlExpressionType.Identifier; }
        }

        public string Name
        {
            get { return _name; }
        }

        protected override Expression Accept(ExpressionVisitor visitor)
        {
            var type = visitor as CqlExpressionVisitor;

            if (type != null)
            {
                return type.VisitIdentifier(this);
            }

            return base.Accept(visitor);
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            return this;
        }

        public override string ToString()
        {
            return string.Format("Identifier({0})", Name);
        }

        public override bool Equals(object obj)
        {
            var type = obj as IdentifierExpression;
            if (type != null)
            {
                return type._name.Equals(_name);
            }
            return false;
        }

        public override int GetHashCode()
        {
            return _name.GetHashCode();
        }
    }
}