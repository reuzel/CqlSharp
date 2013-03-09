using System;

namespace CqlSharp.Serialization
{
    /// <summary>
    /// Annotates a field or property to have it map to a specific column, and optionally table and keyspace
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class CqlColumnAttribute : Attribute
    {
        private readonly string _column;

        public CqlColumnAttribute(string column)
        {
            _column = column;
        }

        public string Table { get; set; }
        public string KeySpace { get; set; }

        public string Column
        {
            get { return _column; }
        }
    }
}