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

        public String ReadNextIdentifier()
        {
            SkipBlank();
            int i = index;
            bool hasDot = false;
            while (!IsEOS() && IsIdentifierChar(_typeName[index]))
            {
                hasDot |= (_typeName[index] == '.');
                index++;
            }

            if (i == index)
                throw new CqlException(string.Format("Error parsing type {0}. Expected an identifier at position {1}", _typeName, index));

            return hasDot ? _typeName.Substring(i, index - i) : "org.apache.cassandra.db.marshal." + _typeName.Substring(i, index - i);
        }

        public char ReadNextChar()
        {
            SkipBlank();
            return _typeName[index++];
        }

        public CqlType ReadCqlType()
        {
            string type = ReadNextIdentifier();
            
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