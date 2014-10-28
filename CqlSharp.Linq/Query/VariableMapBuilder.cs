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
using System.Collections.Generic;
using System.Linq.Expressions;

namespace CqlSharp.Linq.Query
{
    /// <summary>
    ///   Finds all variables in a select expression, and maps them to the corresponding (compiled-query) function arguments.
    /// </summary>
    internal class VariableMapBuilder : CqlExpressionVisitor
    {
        private List<int> _parameterMap;

        public List<int> BuildParameterMap(Expression expression)
        {
            _parameterMap = new List<int>();
            Visit(expression);
            return _parameterMap;
        }

        public override Expression VisitTerm(TermExpression term)
        {
            if (term.NodeType == (ExpressionType)CqlExpressionType.Variable)
            {
                //the term is a variable, add the original argument order nr to the map
                _parameterMap.Add(term.Order);
            }

            return term;
        }
    }
}