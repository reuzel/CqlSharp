using System;
using System.Reflection;

namespace CqlSharp.Serialization
{
    public interface ICqlColumnInfo
    {
        /// <summary>
        ///   Gets the column name.
        /// </summary>
        /// <value> The name. </value>
        string Name { get; }

        /// <summary>
        ///   Gets the CQL type.
        /// </summary>
        /// <value> The type. </value>
        CqlType CqlType { get; }

        /// <summary>
        ///   Gets the .NET type.
        /// </summary>
        /// <value> The type. </value>
        Type Type { get; }

        /// <summary>
        ///   Gets the order/index of a key column.
        /// </summary>
        /// <value> The order. </value>
        int? Order { get; }

        /// <summary>
        ///   Gets a value indicating whether this column is part of the partition key.
        /// </summary>
        /// <value> <c>true</c> if this column is part of the partition key; otherwise, <c>false</c> . </value>
        bool IsPartitionKey { get; }

        /// <summary>
        ///   Gets a value indicating whether this column is part of the clustering key.
        /// </summary>
        /// <value> <c>true</c> if this column is part of the clustering key; otherwise, <c>false</c> . </value>
        bool IsClusteringKey { get; }

        /// <summary>
        ///   Gets a value indicating whether this column is indexed.
        /// </summary>
        /// <value> <c>true</c> if this column is indexed; otherwise, <c>false</c> . </value>
        bool IsIndexed { get; }

        /// <summary>
        ///   Gets the name of the index (if any).
        /// </summary>
        /// <value> The name of the index. </value>
        string IndexName { get; }

        /// <summary>
        ///   Gets the member information.
        /// </summary>
        /// <value> The member information. </value>
        MemberInfo MemberInfo { get; }
    }
}