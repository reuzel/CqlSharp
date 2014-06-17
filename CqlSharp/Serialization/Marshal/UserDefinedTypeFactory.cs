using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Serialization.Marshal
{
    class UserDefinedTypeFactory : ITypeFactory
    {
        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.UserType"; }
        }

        public CqlType CreateType(params object[] args)
        {
            string keyspace = (string)args[0];
            string name = (string)args[1];
            IEnumerable<string> fieldNames = (IEnumerable<string>)args[2];
            IEnumerable<CqlType> types = (IEnumerable<CqlType>)args[3];
            
            return new UserDefinedType(keyspace, name, fieldNames, types);
        }

        public CqlType CreateType(TypeParser parser)
        {
            string keyspace = parser.ReadNextIdentifier();
            parser.SkipBlankAndComma();
            string name = parser.ReadNextIdentifier().DecodeHex();

            List<string> fieldNames = new List<string>();
            List<CqlType> fieldTypes = new List<CqlType>();

            while(parser.SkipBlankAndComma())
            {
                if(parser.Peek() == ')')
                {
                    return new UserDefinedType(keyspace, name, fieldNames, fieldTypes);
                }

                string fieldName = parser.ReadNextIdentifier().DecodeHex();

                if (parser.ReadNextChar() != ':')
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
            var instanceProperty = accessorType.GetField("Instance", BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy);
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
