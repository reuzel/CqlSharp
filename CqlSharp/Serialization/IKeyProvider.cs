using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace CqlSharp.Serialization
{
    /// <summary>
    /// Utility class to enable setting of key flags
    /// </summary>
    internal interface IKeyMember
    {

        /// <summary>
        ///   Gets the member information.
        /// </summary>
        /// <value> The member information. </value>
        MemberInfo MemberInfo { get; }

        /// <summary>
        /// Gets or sets a value indicating whether this instance is partition key.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is partition key; otherwise, <c>false</c>.
        /// </value>
        bool IsPartitionKey { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this instance is clustering key.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is clustering key; otherwise, <c>false</c>.
        /// </value>
        bool IsClusteringKey { get; set; }
    }
}
