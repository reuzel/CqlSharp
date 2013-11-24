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

using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace CqlSharp.Linq
{
    /// <summary>
    ///   Represents a list of
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

        public string GetQueryText(Expression expression)
        {
            return ParseExpression(expression).Item1;
        }

        private object Execute(Expression expression)
        {
            var result = ParseExpression(expression);

            Delegate projector = result.Item2.Compile();
            return Activator.CreateInstance(
                typeof(ProjectionReader<>).MakeGenericType(result.Item2.Type),
                BindingFlags.Instance | BindingFlags.NonPublic, null,
                new object[] { this, result.Item1, projector },
                null
                );
        }

        internal Tuple<string, LambdaExpression> ParseExpression(Expression expression)
        {
            //evaluate all partial expressions (get rid of reference noise)
            var cleanedExpression = PartialVisitor.Evaluate(expression);

            //translate the expression to a cql expression and corresponding projection
            var translation = new ExpressionTranslator().Translate(cleanedExpression);

            //get rid of unnecessary column identifers
            translation = new ColumnReducer().ReduceColumns(translation);

            //generate cql text
            var cql = new CqlTextBuilder().Build(translation.Select);

            //get a projection delegate
            var projector = new ProjectorBuilder().BuildProjector(translation.Projection);

            //return translation results
            return new Tuple<string, LambdaExpression>(cql, projector);
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