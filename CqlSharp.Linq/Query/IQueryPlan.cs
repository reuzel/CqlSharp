using System.Collections.Generic;

namespace CqlSharp.Linq.Query
{
    internal interface IQueryPlan
    {
        /// <summary>
        /// Executes the query plan on the specified context.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="args">parameter values to be used to run a (prepared) query</param>
        /// <returns></returns>
        TResult Execute<TResult>(CqlContext context, object[] args);

        /// <summary>
        /// Gets the consistency.
        /// </summary>
        /// <value>
        /// The consistency.
        /// </value>
        CqlConsistency? Consistency { get; }

        /// <summary>
        /// Gets the size of the batch.
        /// </summary>
        /// <value>
        /// The size of the batch.
        /// </value>
        int? PageSize { get; }

        /// <summary>
        ///   Gets the CQL query string.
        /// </summary>
        /// <value> The CQL query string. </value>
        string Cql { get; }

        /// <summary>
        /// Gets the mapping of query variables to function arguments
        /// </summary>
        /// <value>
        /// The variable map.
        /// </value>
        List<int> VariableMap { get; }

        /// <summary>
        ///   Gets a value indicating whether the results of the query can be tracked for changes by the used context
        /// </summary>
        /// <value> <c>true</c> if [can track changes]; otherwise, <c>false</c> . </value>
        bool CanTrackChanges { get; }
    }
}