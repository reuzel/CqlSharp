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

using CqlSharp.Linq.Expressions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace CqlSharp.Linq
{
    /// <summary>
    ///   A representation of a Cql database (keyspace)
    /// </summary>
    public abstract class CqlContext : IQueryProvider, IDisposable
    {
        private string _connectionString;

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlContext" /> class.
        /// </summary>
        /// <param name="initializeTables"> indicates wether the table properties are to be automatically initialized </param>
        protected CqlContext(bool initializeTables = true)
        {
            SkipExecute = false;

            if (initializeTables)
                InitializeTables();
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlContext" /> class.
        /// </summary>
        /// <param name="connectionString"> The connection string. </param>
        /// <param name="initializeTables"> indicates wether the table properties are to be automatically initialized </param>
        protected CqlContext(string connectionString, bool initializeTables = true)
        {
            SkipExecute = false;

            _connectionString = connectionString;
            if (initializeTables)
                InitializeTables();
        }

        /// <summary>
        ///   Gets the connection string.
        /// </summary>
        /// <value> The connection string. </value>
        public string ConnectionString
        {
            get
            {
                if (_connectionString == null)
                    _connectionString = GetType().Name;

                return _connectionString;
            }

            set { _connectionString = value; }
        }

        /// <summary>
        /// Gets or sets the log where executed CQL queries are written to
        /// </summary>
        /// <value>
        /// The log.
        /// </value>
        public TextWriter Log { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether execution of the query is skipped. This is for debugging purposes.
        /// </summary>
        /// <value>
        ///   <c>true</c> if execution is skipped; otherwise, <c>false</c>.
        /// </value>
        public bool SkipExecute { get; set; }

        #region IDisposable Members

        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
        }

        #endregion

        /// <summary>
        ///   Initializes the tables of this context.
        /// </summary>
        private void InitializeTables()
        {
            var properties = GetType().GetProperties();
            foreach (var property in properties)
            {
                var propertyType = property.PropertyType;
                if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(CqlTable<>))
                {
                    var table = Activator.CreateInstance(propertyType, this);
                    property.SetValue(this, table);
                }
            }
        }

        private object Execute(Expression expression)
        {
            var result = ParseExpression(expression);

            //log the query
            if (Log != null)
                Log.WriteLine(result.Cql);

            //return default values of execution is to be skipped
            if (SkipExecute)
            {
                //return empty array
                if (result.ResultFunction == null)
                    return Array.CreateInstance(result.Projector.ReturnType, 0);

                //return default value or null
                return result.Projector.ReturnType.DefaultValue();
            }

            Delegate projector = result.Projector.Compile();

            var enm = (IEnumerable<object>)Activator.CreateInstance(
                        typeof(ProjectionReader<>).MakeGenericType(result.Projector.ReturnType),
                        BindingFlags.Instance | BindingFlags.NonPublic, null,
                        new object[] { this, result.Cql, projector },
                        null
                        );

            if (result.ResultFunction != null)
                return result.ResultFunction.Invoke(enm);

            return enm;
        }

        internal class ParseResult
        {
            public string Cql { get; set; }
            public LambdaExpression Projector { get; set; }
            public ResultFunction ResultFunction { get; set; }
        }

        internal ParseResult ParseExpression(Expression expression)
        {
            Debug.WriteLine("Original Expression: " + expression);

            //evaluate all partial expressions (get rid of reference noise)
            var cleanedExpression = PartialEvaluator.Evaluate(expression, CanBeEvaluatedLocally);
            Debug.WriteLine("Cleaned Expression: " + cleanedExpression);

            //translate the expression to a cql expression and corresponding projection
            var translation = new ExpressionTranslator().Translate(cleanedExpression);
            
            //generate cql text
            var cql = new CqlTextBuilder().Build(translation.Select);
            Debug.WriteLine("Generated CQL: " + cql);

            //get a projection delegate
            var projector = new ProjectorBuilder().BuildProjector(translation.Projection);
            Debug.WriteLine("Generated Projector: " + projector);
            Debug.WriteLine("Result processor: " + (translation.ResultFunction!=null ? translation.ResultFunction.GetMethodInfo().ToString() : "<none>"));

            //return translation results
            return new ParseResult { Cql = cql, Projector = projector, ResultFunction = translation.ResultFunction };
        }

        private bool CanBeEvaluatedLocally(Expression expression)
        {
            var cex = expression as ConstantExpression;
            if (cex != null)
            {
                var query = cex.Value as IQueryable;
                if (query != null && query.Provider == this)
                    return false;

            }

            return expression.NodeType != ExpressionType.Parameter &&
                   expression.NodeType != ExpressionType.Lambda;
        }

        #region IQueryProvider implementation

        /// <summary>
        ///   Creates the query.
        /// </summary>
        /// <typeparam name="TElement"> The type of the element. </typeparam>
        /// <param name="expression"> The expression. </param>
        /// <returns> </returns>
        IQueryable<TElement> IQueryProvider.CreateQuery<TElement>(Expression expression)
        {
            return new CqlTable<TElement>(this, expression);
        }

        /// <summary>
        ///   Constructs an <see cref="T:System.Linq.IQueryable" /> object that can evaluate the query represented by a specified expression tree.
        /// </summary>
        /// <param name="expression"> An expression tree that represents a LINQ query. </param>
        /// <returns> An <see cref="T:System.Linq.IQueryable" /> that can evaluate the query represented by the specified expression tree. </returns>
        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            Type elementType = TypeSystem.GetElementType(expression.Type);
            try
            {
                return
                    (IQueryable)
                    Activator.CreateInstance(typeof(CqlTable<>).MakeGenericType(elementType),
                                             new object[] { this, expression });
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        /// <summary>
        ///   Executes the specified expression.
        /// </summary>
        /// <typeparam name="TResult"> The type of the result. </typeparam>
        /// <param name="expression"> The expression. </param>
        /// <returns> </returns>
        TResult IQueryProvider.Execute<TResult>(Expression expression)
        {
            return (TResult)Execute(expression);
        }

        /// <summary>
        ///   Executes the query represented by a specified expression tree.
        /// </summary>
        /// <param name="expression"> An expression tree that represents a LINQ query. </param>
        /// <returns> The value that results from executing the specified query. </returns>
        object IQueryProvider.Execute(Expression expression)
        {
            return Execute(expression);
        }

        #endregion
    }
}