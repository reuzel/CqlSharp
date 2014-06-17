using CqlSharp.Serialization.Marshal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Serialization
{
    /// <summary>
    /// Annotates a class, indicating how it should be serialized.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class CqlCustomTypeAttribute : Attribute
    {
        private readonly Type _factory;

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlCustomTypeAttribute"/> class.
        /// </summary>
        /// <param name="cqlTypeFactory">The CQL type factory.</param>
        /// <exception cref="System.ArgumentException">Type provided to CqlCustomType attribute must be a ITypeFactory implementation</exception>
        public CqlCustomTypeAttribute(Type cqlTypeFactory)
        {
            if (!cqlTypeFactory.GetInterfaces().Contains(typeof(ITypeFactory)))
                throw new ArgumentException("Type provided to CqlCustomType attribute must be a ITypeFactory implementation");
            
            _factory = cqlTypeFactory;
        }

        /// <summary>
        /// Creates the type factory for the given class
        /// </summary>
        public ITypeFactory CreateFactory() 
        { 
            return (ITypeFactory)Activator.CreateInstance(_factory); 
        } 
    }
}
