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

using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using CqlSharp.Linq.Expressions;

namespace CqlSharp.Linq
{
    internal abstract class BuilderBase : CqlExpressionVisitor
    {
        private readonly Dictionary<Expression, Expression> _map;

        protected BuilderBase()
        {
            _map = new Dictionary<Expression, Expression>();
        }

        /// <summary>
        ///   Adds mappings between the arguments of the provided lambda and its replacement expressions
        /// </summary>
        /// <param name="lambda"> The lambda. </param>
        /// <param name="replacements"> The replacements. </param>
        protected void MapLambdaParameters(LambdaExpression lambda, params Expression[] replacements)
        {
            //map the lambdas input parameter to the "current" projection, allowing
            //the parameter to be replaced when it occurs
            for (int i = 0; i < lambda.Parameters.Count; i++)
                _map[lambda.Parameters[i]] = replacements[i];
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            //replace parameter with corresponding projection if known
            Expression projection;
            if (_map.TryGetValue(node, out projection))
                return projection;

            return node;
        }

        /// <summary>
        ///   Attempt to get the assigned value expression of the accessed property or field
        /// </summary>
        /// <param name="node"> </param>
        /// <returns> </returns>
        protected override Expression VisitMember(MemberExpression node)
        {
            //visit the expression first, to make sure parameter values are replaced,
            //or to move through hierarchies of member access
            Expression source = Visit(node.Expression);

            switch (source.NodeType)
            {
                case ExpressionType.MemberInit:
                    //object/class initialization of a known class
                    var initExpression = (MemberInitExpression) source;

                    //search for the assignment of a value to the given member and return the value if found
                    foreach (var binding in initExpression.Bindings)
                    {
                        if (binding.BindingType == MemberBindingType.Assignment &&
                            MembersMatch(binding.Member, node.Member))
                        {
                            return ((MemberAssignment) binding).Expression;
                        }
                    }
                    break;
                case ExpressionType.New:
                    //object/class initialization of a anonymous class
                    var nex = (NewExpression) source;

                    //search for the assignment of a value to the given member and return the value if found
                    for (int i = 0, n = nex.Members.Count; i < n; i++)
                    {
                        if (MembersMatch(nex.Members[i], node.Member))
                            return nex.Arguments[i];
                    }
                    break;
            }

            if (source == node.Expression)
            {
                return node;
            }

            return Expression.MakeMemberAccess(source, node.Member);
        }

        private bool MembersMatch(MemberInfo a, MemberInfo b)
        {
            if (a == b)
            {
                return true;
            }

            if (a is MethodInfo && b is PropertyInfo)
            {
                return a == ((PropertyInfo) b).GetGetMethod();
            }

            if (a is PropertyInfo && b is MethodInfo)
            {
                return ((PropertyInfo) a).GetGetMethod() == b;
            }

            return false;
        }
    }
}