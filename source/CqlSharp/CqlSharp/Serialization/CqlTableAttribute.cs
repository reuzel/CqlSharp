using System;

namespace CqlSharp.Serialization
{
    /// <summary>
    /// Annotates a class to have it map to a specific table and optionally keyspace
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class CqlTableAttribute : Attribute
    {
        private readonly string _table;

        public CqlTableAttribute(string table)
        {
            _table = table;
        }

        public string Table
        {
            get { return _table; }
        }

        public string Keyspace { get; set; }
    }
}