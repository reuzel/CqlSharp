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

using System;
using System.Collections.Generic;
using System.Reflection;

namespace CqlSharp.Linq.Query
{
    /// <summary>
    ///   Container of all logic to execute a Linq query
    /// </summary>
    internal class QueryPlan<TProjection> : IQueryPlan
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="QueryPlan{TProjection}" /> class.
        /// </summary>
        /// <param name="cql">The CQL query</param>
        /// <param name="variableMap">Map of query variables indexes to argument positions</param>
        /// <param name="projector">The projector of the query datareader to object results</param>
        /// <param name="aggregatorFunc">The function to aggregates the result objects into a different form.</param>
        /// <param name="canTrackChanges">if set to <c>true</c> [can track changes].</param>
        /// <param name="consistency">The consistency.</param>
        /// <param name="batchSize">Size of the batch.</param>
        public QueryPlan(string cql, List<int> variableMap, Delegate projector, Delegate aggregatorFunc, bool canTrackChanges, CqlConsistency? consistency, int? batchSize)
        {
            Consistency = consistency;
            PageSize = batchSize;
            Cql = cql;
            VariableMap = variableMap;
            Projector = projector;
            CanTrackChanges = canTrackChanges;
            Aggregator = aggregatorFunc;
        }

        /// <summary>
        /// Gets the consistency.
        /// </summary>
        /// <value>
        /// The consistency.
        /// </value>
        public CqlConsistency? Consistency { get; private set; }

        /// <summary>
        /// Gets the size of the batch.
        /// </summary>
        /// <value>
        /// The size of the batch.
        /// </value>
        public int? PageSize { get; private set; }

        /// <summary>
        ///   Gets the CQL query string.
        /// </summary>
        /// <value> The CQL query string. </value>
        public string Cql { get; private set; }

        /// <summary>
        /// Gets the mapping of query variables to function arguments
        /// </summary>
        /// <value>
        /// The variable map.
        /// </value>
        public List<int> VariableMap { get; private set; }

        /// <summary>
        ///   Gets the projector translating database results into an object structure
        /// </summary>
        /// <value> The projector. </value>
        public Delegate Projector { get; private set; }

        /// <summary>
        ///   Gets a value indicating whether the results of the query can be tracked for changes by the used context
        /// </summary>
        /// <value> <c>true</c> if [can track changes]; otherwise, <c>false</c> . </value>
        public bool CanTrackChanges { get; private set; }

        /// <summary>
        ///   Gets the function that, if set, aggregates the results into the required form
        /// </summary>
        /// <value> The result function. </value>
        public Delegate Aggregator { get; private set; }

        /// <summary>
        /// Executes the query plan on the specified context.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="args">parameter values to be used to run a (prepared) query</param>
        /// <returns></returns>
        public TResult Execute<TResult>(CqlContext context, object[] args)
        {

            IEnumerable<TProjection> reader;
            if (CanTrackChanges && context.TrackChanges)
            {
                //use reflection to instantiate as class restrictions do not allow this thing to be instantiated directly
                reader = (IEnumerable<TProjection>)Activator.CreateInstance(
                    typeof(TrackingReader<>).MakeGenericType(typeof(TProjection)),
                    BindingFlags.Instance | BindingFlags.Public, null,
                    new object[] { context, this, args },
                    null);
            }
            else
            {
                reader = new ProjectionReader<TProjection>(context, this, args);
            }

            if (Aggregator != null)
            {
                var aggregator = (Func<IEnumerable<TProjection>, TResult>)Aggregator;
                return aggregator(reader);
            }

            return (TResult)reader;
        }
    }
}