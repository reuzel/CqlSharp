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
using CqlSharp.Extensions;

namespace CqlSharp.Serialization.Marshal
{
    public class TypeParser
    {
        private readonly string _typeName;
        private int index;

        public TypeParser(string typeName)
        {
            _typeName = typeName;
            index = 0;
        }

        public char Peek()
        {
            return _typeName[index];
        }

        public bool IsEOS()
        {
            return index >= _typeName.Length;
        }

        public void SkipBlank()
        {
            while(!IsEOS() && IsBlank(Peek()))
                index++;
        }

        /// <summary>
        /// Skips the blanks and a single comma.
        /// </summary>
        /// <returns>true if more data is available after this call</returns>
        public bool SkipBlankAndComma()
        {
            bool commaFound = false;
            while(!IsEOS())
            {
                char c = _typeName[index];
                if(c == ',')
                {
                    if(commaFound)
                        return true;
                    commaFound = true;
                }
                else if(!IsBlank(c))
                    return true;
                index++;
            }
            return false;
        }

        private static bool IsBlank(char c)
        {
            return c == ' ' || c == '\t' || c == '\n';
        }

        private static bool IsIdentifierChar(char c)
        {
            return (c >= '0' && c <= '9')
                   || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                   || c == '-' || c == '+' || c == '.' || c == '_' || c == '&';
        }

        public String ReadNextIdentifier(bool expandNamespace = true)
        {
            SkipBlank();
            int i = index;
            while(!IsEOS() && IsIdentifierChar(_typeName[index]))
                index++;

            if(i == index)
            {
                throw new CqlException(string.Format("Error parsing type {0}. Expected an identifier at position {1}",
                                                     _typeName, index));
            }

            return _typeName.Substring(i, index - i);
        }

        public char ReadNextChar()
        {
            SkipBlank();
            return _typeName[index++];
        }

        public CqlType ReadCqlType()
        {
            string type = ReadNextIdentifier();

            if(type.IndexOf('.') < 0)
                type = "org.apache.cassandra.db.marshal." + type;

            var typeFactory =
                Loader.Extensions.TypeFactories.Find(
                    factory => string.Equals(factory.TypeName, type, StringComparison.OrdinalIgnoreCase));

            if(typeFactory == null)
                throw new CqlException(string.Format("Type {0} is not supported", type));

            CqlType cqlType;
            SkipBlank();
            if(!IsEOS() && Peek() == '(')
            {
                ReadNextChar();
                cqlType = typeFactory.CreateType(this);
                if(ReadNextChar() != ')')
                {
                    throw new CqlException(
                        string.Format("Error parsing type string \"{0}\", expected ')' at position {1}.", _typeName,
                                      index));
                }
            }
            else
                cqlType = typeFactory.CreateType();

            return cqlType;
        }
    }
}