
namespace CqlSharp
{
    /// <summary>
    /// Contains constants representing the names of the columns of the DataTable
    /// obtained via the <see cref="M:CqlSharp.CqlDataReader.GetSchemaTable" /> method
    /// of a <see cref="T:CqlSharp.CqlDataReader"/> instance.
    /// </summary>
    public static class CqlSchemaTableColumnNames
    {
        /// <summary>
        /// The column ordinal, or the position in the DataReader values
        /// </summary>
        public const string ColumnOrdinal = "ColumnOrdinal";

        /// <summary>
        /// The key space name
        /// </summary>
        public const string KeySpaceName = "KeySpaceName";

        /// <summary>
        /// The table name
        /// </summary>
        public const string TableName = "TableName";

        /// <summary>
        /// The column name
        /// </summary>
        public const string ColumnName = "ColumnName";

        /// <summary>
        /// The CQL type
        /// </summary>
        public const string CqlType = "CqlType";

        /// <summary>
        /// The collection key type
        /// </summary>
        public const string CollectionKeyType = "CollectionKeyType";

        /// <summary>
        /// The collection value type
        /// </summary>
        public const string CollectionValueType = "CollectionValueType";

        /// <summary>
        /// The .NET that will be used to represent the values of the column.
        /// </summary>
        public const string Type = "Type";
    }
}
