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
using System.Collections.Generic;
using System.Linq;

namespace CqlSharp.Serialization.Marshal
{
    public class TupleTypeFactory : ITypeFactory
    {
        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.TupleType"; }
        }

        public CqlType CreateType(params object[] innerTypes)
        {
            if(innerTypes==null || !innerTypes.All(t => t is CqlType))
                throw new ArgumentException("Arguments to create a TupleType must consist of CqlTypes only");
            
            //cast the innerTypes to CqlTypes
            var subTypes = innerTypes.Cast<CqlType>().ToList();

            return CreateType(subTypes);
        }

        public CqlType CreateType(TypeParser parser)
        {
            var types = new List<CqlType>();
            parser.SkipBlank();
            while(!parser.IsEOS() && parser.Peek() != ')')
            {
                types.Add(parser.ReadCqlType());
                parser.SkipBlankAndComma();
            }
            
            return CreateType(types);
        }

        /// <summary>
        /// Creates the type.
        /// </summary>
        /// <param name="cqlTypes">The CQL types.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentException">The number of subtypes used to create a Tuple must be between 1 and 8 (inclusive)</exception>
        private CqlType CreateType(List<CqlType> cqlTypes)
        {
            int typeCount = cqlTypes.Count;

            if (typeCount > 8 || typeCount < 1)
                throw new ArgumentException("The number of subtypes used to create a Tuple must be between 1 and 8 (inclusive)");
            
            //get the system.Tuple version that represent the provided tuple
            var tupleArgumentType = TypeExtensions.TupleTypes[typeCount - 1].MakeGenericType(cqlTypes.Select(st => st.Type).ToArray());

            //define the TupleType necessary
            var tupleType = typeof(TupleType<>).MakeGenericType(tupleArgumentType);

            //create the TupleType
            return (CqlType)Activator.CreateInstance(tupleType, cqlTypes);
        }

        public CqlType CreateType(Type type)
        {
            //define the TupleType necessary
            var tupleType = typeof(TupleType<>).MakeGenericType(type);

            //create the TupleType
            return (CqlType)Activator.CreateInstance(tupleType);
        }
    }
}