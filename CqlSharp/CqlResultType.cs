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

namespace CqlSharp
{
    /// <summary>
    ///   Types of result of a Cql query
    /// </summary>
    public enum CqlResultType : short
    {
        /// <summary>
        ///   Represents result of queries with no results (e.g. insert or update)
        /// </summary>
        Void = 0x0001,

        /// <summary>
        ///   Represents result of select queries
        /// </summary>
        Rows = 0x0002,

        /// <summary>
        ///   Represents result of use queries
        /// </summary>
        SetKeyspace = 0x0003,

        /// <summary>
        ///   Represents the result of a prepare query
        /// </summary>
        Prepared = 0x0004,

        /// <summary>
        ///   Represents result of ResultMetaData changing queries (create/alter)
        /// </summary>
        SchemaChange = 0x0005
    }
}