// CqlSharp - CqlSharp
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
using System.Linq;

namespace CqlSharp.Serialization
{
    /// <summary>
    /// Marks that instances of the given class can be converted to other types using the provided TypeConverter
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class CqlTypeConverterAttribute : Attribute
    {
        private readonly Type _converter;

        public Type Converter
        {
            get { return _converter; }
        }

        public CqlTypeConverterAttribute(Type converter)
        {
            if(
                !converter.GetInterfaces()
                          .Any(ifc => ifc.IsGenericType && ifc.GetGenericTypeDefinition() == typeof(ITypeConverter<>)))
                throw new ArgumentException("Converter type must implement ITypeConverter<>");

            _converter = converter;
        }
    }
}