using CqlSharp.Extensions;
using System;

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
            while (!IsEOS() && IsBlank(Peek()))
                index++;
        }

        /// <summary>
        /// Skips the blanks and a single comma.
        /// </summary>
        /// <returns>true if more data is available after this call</returns>
        public bool SkipBlankAndComma()
        {
            bool commaFound = false;
            while (!IsEOS())
            {
                char c = _typeName[index];
                if (c == ',')
                {
                    if (commaFound)
                        return true;
                    else
                        commaFound = true;
                }
                else if (!IsBlank(c))
                {
                    return true;
                }
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
            while (!IsEOS() && IsIdentifierChar(_typeName[index]))
            {
                index++;
            }

            if (i == index)
                throw new CqlException(string.Format("Error parsing type {0}. Expected an identifier at position {1}", _typeName, index));

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

            if(type.IndexOf('.')<0)
                type = "org.apache.cassandra.db.marshal." + type;
            
            var typeFactory = Extensions.Loader.Extensions.TypeFactories.Find(factory => factory.TypeName.Equals(type, StringComparison.OrdinalIgnoreCase));

            if (typeFactory == null)
                throw new CqlException(string.Format("Type {0} is not supported", type));

            CqlType cqlType;
            SkipBlank();
            if (!IsEOS() && Peek() == '(')
            {
                ReadNextChar();
                cqlType = typeFactory.CreateType(this);
                if (ReadNextChar() != ')')
                    throw new CqlException(string.Format("Error parsing type string \"{0}\", expected ')' at position {1}.", _typeName, index));
            }
            else
                cqlType = typeFactory.CreateType();

            return cqlType;
        }
    }
}