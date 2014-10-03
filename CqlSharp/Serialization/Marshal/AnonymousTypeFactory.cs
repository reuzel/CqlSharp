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

namespace CqlSharp.Serialization.Marshal
{
    public class AnonymousTypeFactory : ITypeFactory
    {
        public string TypeName
        {
            get { throw new NotSupportedException("Anonymous types do not have a name"); }
        }

        public CqlType CreateType(params object[] innerTypes)
        {
            throw new NotSupportedException("Anonymous types require a .NET type to be instantiated");
        }

        public CqlType CreateType(TypeParser parser)
        {
            throw new NotSupportedException("Anonymous types can't be parsed. (As they have no Cassandra counterpart)");
        }

        public CqlType CreateType(Type type)
        {
            Type anType = typeof(AnonymousType<>).MakeGenericType(type);
            return (CqlType)Activator.CreateInstance(anType);
        }
    }
}