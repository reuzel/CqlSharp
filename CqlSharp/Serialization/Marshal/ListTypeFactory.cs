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
using System.Collections.Concurrent;

namespace CqlSharp.Serialization.Marshal
{
    public class ListTypeFactory : ITypeFactory
    {
        private static readonly ConcurrentDictionary<CqlType, CqlType> _types =
            new ConcurrentDictionary<CqlType, CqlType>();

        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.ListType"; }
        }

        public CqlType CreateType(params object[] innerTypes)
        {
            var innerType = innerTypes[0] as CqlType;
            if(innerType == null)
                throw new CqlException("Need a CqlType as parameter when constructing a ListType");

            return CreateType(innerType);
        }

        public CqlType CreateType(CqlType innerType)
        {
            return _types.GetOrAdd(innerType, type =>
                                                  (CqlType)
                                                  Activator.CreateInstance(
                                                      typeof(ListType<>).MakeGenericType(type.Type), type)
                );
        }

        public CqlType CreateType(TypeParser parser)
        {
            var innerType = parser.ReadCqlType();
            return CreateType(innerType);
        }

        public CqlType CreateType(Type type)
        {
            return CreateType(CqlType.CreateType(type.GetGenericArguments()[0]));
        }
    }
}