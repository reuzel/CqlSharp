using System;
using System.Collections.Generic;

namespace CqlSharp.Linq
{
    /// <summary>
    /// Executes a CQL query and transforms the results into object using a projector
    /// </summary>
    /// <typeparam name="T"></typeparam>
    class ProjectionReader<T> : IEnumerable<T>
    {
        private readonly CqlContext _context;
        private readonly string _cql;
        private readonly Func<CqlDataReader, T> _projector;

        /// <summary>
        /// Initializes a new instance of the <see cref="ProjectionReader{T}"/> class.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="cql">The CQL.</param>
        /// <param name="projector">The projector.</param>
        /// <exception cref="System.ArgumentNullException">
        /// context
        /// or
        /// cql
        /// or
        /// projector
        /// </exception>
        public ProjectionReader(CqlContext context, string cql, Func<CqlDataReader, T> projector)
        {
            if (context == null) throw new ArgumentNullException("context");
            if (cql == null) throw new ArgumentNullException("cql");
            if (projector == null) throw new ArgumentNullException("projector");

            _context = context;
            _cql = cql;
            _projector = projector;
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Collections.Generic.IEnumerator`1" /> that can be used to iterate through the collection.
        /// </returns>
        public IEnumerator<T> GetEnumerator()
        {
            using (var connection = new CqlConnection(_context.ConnectionString))
            {
                connection.Open();

                var command = new CqlCommand(connection, _cql);
                using (var reader = command.ExecuteReader())
                {
                    while(reader.Read())
                    {
                        yield return _projector(reader);
                    }
                }
            }
        }

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Collections.IEnumerator" /> object that can be used to iterate through the collection.
        /// </returns>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}