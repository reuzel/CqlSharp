using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Serialization
{
    /// <summary>
    /// Marks that instances of the given class can be converted to other types using the provided TypeConverter
    /// </summary>
    /// 
    [AttributeUsage(AttributeTargets.Class)]
    public class CqlTypeConverterAttribute : Attribute
    {
        private Type _converter;

        public Type Converter
        {
            get { return _converter; }
        }

        public CqlTypeConverterAttribute(Type converter)
        {
            if(!converter.GetInterfaces().Any(ifc => ifc.IsGenericType && ifc.GetGenericTypeDefinition() == typeof(ITypeConverter<>)))
                throw new ArgumentException("Converter type must implement ITypeConverter<>");

            _converter = converter;
        }
    }
}
