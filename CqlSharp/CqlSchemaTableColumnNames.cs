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
    ///   Contains constants representing the names of the columns of the DataTable
    ///   obtained via the <see cref="M:CqlSharp.CqlDataReader.GetSchemaTable" /> method
    ///   of a <see cref="T:CqlSharp.CqlDataReader" /> instance.
    /// </summary>
    public static class CqlSchemaTableColumnNames
    {
        /// <summary>
        ///   The column ordinal, or the position in the DataReader values
        /// </summary>
        public const string ColumnOrdinal = "ColumnOrdinal";

        /// <summary>
        ///   The key space name
        /// </summary>
        public const string KeySpaceName = "KeySpaceName";

        /// <summary>
        ///   The table name
        /// </summary>
        public const string TableName = "TableName";

        /// <summary>
        ///   The column name
        /// </summary>
        public const string ColumnName = "ColumnName";

        /// <summary>
        ///   The CQL typeCode
        /// </summary>
        public const string CqlType = "CqlTypeCode";

        /// <summary>
        ///   The CQL custom typeCode name
        /// </summary>
        public const string CustomType = "CustomType";

        /// <summary>
        ///   The collection key typeCode
        /// </summary>
        public const string CollectionKeyType = "CollectionKeyType";

        /// <summary>
        ///   The collection value typeCode
        /// </summary>
        public const string CollectionValueType = "CollectionValueType";

        /// <summary>
        ///   The .NET that will be used to represent the values of the column.
        /// </summary>
        public const string Type = "Type";
    }
}