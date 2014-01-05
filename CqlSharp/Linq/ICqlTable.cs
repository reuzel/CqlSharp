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
using System.Reflection;

namespace CqlSharp.Linq
{
    /// <summary>
    ///   Utility interface to access CqlTables in a non-generic way
    /// </summary>
    public interface ICqlTable
    {
        /// <summary>
        ///   Gets the column names.
        /// </summary>
        /// <value> The column names. </value>
        Dictionary<MemberInfo, string> ColumnNames { get; }

        /// <summary>
        ///   Gets the name of the Table.
        /// </summary>
        /// <value> The name. </value>
        string Name { get; }

        /// <summary>
        ///   Gets the type of entity contained by this table.
        /// </summary>
        /// <value> The type. </value>
        Type Type { get; }
    }
}