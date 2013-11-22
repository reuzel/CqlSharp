using System;
using System.Collections.Generic;
using System.Reflection;

namespace CqlSharp.Linq
{
    /// <summary>
    /// Utility interface to access CqlTables in a non-generic way
    /// </summary>
    public interface ICqlTable
    {
        /// <summary>
        /// Gets the column names.
        /// </summary>
        /// <value>
        /// The column names.
        /// </value>
        Dictionary<MemberInfo, string> ColumnNames { get; }

        /// <summary>
        /// Gets the name of the Table.
        /// </summary>
        /// <value>
        /// The name.
        /// </value>
        string Name { get; }

        /// <summary>
        /// Gets the type of entity contained by this table.
        /// </summary>
        /// <value>
        /// The type.
        /// </value>
        Type Type { get; }
    }
}