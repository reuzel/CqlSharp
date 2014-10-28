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

namespace CqlSharp.Linq.Expressions
{
    /// <summary>
    ///   Type of expression, explicit for Cql Expression elements
    /// </summary>
    internal enum CqlExpressionType
    {
        SelectStatement = 1000,
        OrderAscending,
        OrderDescending,
        Equal,
        LargerThan,
        LargerEqualThan,
        SmallerThan,
        SmallerEqualThan,
        In,
        Constant,
        Map,
        Set,
        List,
        Variable,
        Function,
        IdentifierSelector,
        FunctionSelector,
        SelectCount,
        SelectColumns,
        Projection,
        SelectAll,
        Database
    }
}