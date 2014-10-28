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

using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace CqlSharp.Linq.Query
{
    /// <summary>
    ///   Extensions supporting various parsing tasks
    /// </summary>
    internal static class ExpressionExtensions
    {
        public static Expression StripQuotes(this Expression e)
        {
            while (e.NodeType == ExpressionType.Quote)
            {
                e = ((UnaryExpression) e).Operand;
            }
            return e;
        }

        public static bool IsIdentityLambda(this Expression e)
        {
            var lambda = e as LambdaExpression;
            if (lambda != null)
            {
                return lambda.Parameters[0].Equals(lambda.Body);
            }
            return false;
        }

        public static bool IsTrue(this Expression e)
        {
            var constant = e as ConstantExpression;
            if (constant != null && constant.Type == typeof (bool))
            {
                return (bool) constant.Value;
            }
            return false;
        }

/*
        public static IList<Expression> GetArrayElements(this Expression e)
        {
            var expressions = new List<Expression>();

            var constant = e as ConstantExpression;
            if (constant != null)
            {
                //reduce inner array to its elements
                if (constant.Type.Implements(typeof (IEnumerable)))
                {
                    foreach (var val in (IEnumerable) constant.Value)
                    {
                        expressions.Add(Expression.Constant(val));
                    }
                    return expressions;
                }

                //single constant, return as array
                return new[] {e};
            }


            var nae = e as NewArrayExpression;
            if (nae == null)
            {
                return new[] {e};
            }

            foreach (var expr in nae.Expressions)
            {
                var convert = expr as UnaryExpression;
                if (convert == null || convert.NodeType != ExpressionType.Convert)
                {
                    expressions.Add(expr);
                    continue;
                }

                expressions.Add(convert.Operand);
            }

            return expressions;
        }
*/
    }
}