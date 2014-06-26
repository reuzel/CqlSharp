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
using System.Diagnostics;
using System.Linq;
using System.Reflection;

namespace CqlSharp.Serialization.Marshal
{
    internal class UserDefinedTypeFactory : ITypeFactory
    {
        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.UserType"; }
        }

        public CqlType CreateType(params object[] args)
        {
            var keyspace = (string)args[0];
            var name = (string)args[1];
            var fieldNames = (IEnumerable<string>)args[2];
            var types = (IEnumerable<CqlType>)args[3];

            return new UserDefinedType(keyspace, name, fieldNames, types);
        }

        public CqlType CreateType(TypeParser parser)
        {
            string keyspace = parser.ReadNextIdentifier();
            parser.SkipBlankAndComma();
            string name = parser.ReadNextIdentifier().DecodeHex();

            var fieldNames = new List<string>();
            var fieldTypes = new List<CqlType>();

            while(parser.SkipBlankAndComma())
            {
                if(parser.Peek() == ')')
                    return new UserDefinedType(keyspace, name, fieldNames, fieldTypes);

                string fieldName = parser.ReadNextIdentifier().DecodeHex();

                if(parser.ReadNextChar() != ':')
                    throw new CqlException("Error parsing UserType arguments: ':' expected after fieldName.");

                CqlType type = parser.ReadCqlType();

                fieldNames.Add(fieldName);
                fieldTypes.Add(type);
            }

            throw new CqlException("Error parsing UserType arguments: unexpected end of string.");
        }

        public CqlType CreateType(Type type)
        {
            //get an accessor to the type
            var accessorType = typeof(ObjectAccessor<>).MakeGenericType(type);
            var instanceProperty = accessorType.GetField("Instance",
                                                         BindingFlags.Public | BindingFlags.Static |
                                                         BindingFlags.FlattenHierarchy);

            Debug.Assert(instanceProperty != null, "instanceProperty != null");
            var accessor = (IObjectAccessor)instanceProperty.GetValue(null);

            //return new UserDefinedType
            return new UserDefinedType(
                accessor.Keyspace,
                accessor.Name,
                accessor.Columns.Select(c => c.Name),
                accessor.Columns.Select(c => c.CqlType)
                );
        }
    }
}