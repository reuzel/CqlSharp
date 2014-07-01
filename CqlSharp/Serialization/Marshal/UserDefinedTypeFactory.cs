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
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using CqlSharp.Protocol;

namespace CqlSharp.Serialization.Marshal
{
    internal class UserDefinedTypeFactory : ITypeFactory
    {
        private static readonly ConcurrentDictionary<string, Type> UserDefinedTypes = new ConcurrentDictionary<string, Type>();
        
        public string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.UserType"; }
        }

        public CqlType CreateType(params object[] args)
        {
            var keyspace = (string)args[0];
            var name = (string)args[1];
            var fieldNames = (List<string>)args[2];
            var fieldTypes = (List<CqlType>)args[3];

            return CreateTypeInternal(keyspace, name, fieldNames, fieldTypes, null);
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
                {
                    return CreateTypeInternal(keyspace, name, fieldNames, fieldTypes, null);
                }

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
            return CreateTypeInternal(
                accessor.Keyspace,
                accessor.Name,
                accessor.Columns.Select(c => c.Name).ToList(),
                accessor.Columns.Select(c => c.CqlType).ToList(),
                type);
        }

        private CqlType CreateTypeInternal(string keyspace, string name, List<string> fieldNames, List<CqlType> fieldTypes, Type reflectedType)
        {
            Type udt = reflectedType;
            if(udt == null)
            {
                var typeId = GetTypeId(keyspace, name, fieldNames, fieldTypes);
                udt = UserDefinedTypes.GetOrAdd(typeId, _ => EmitNewType(keyspace, name, fieldNames, fieldTypes));
            }

            Type userDefinedType = typeof(UserDefinedType<>).MakeGenericType(udt);
            return (CqlType)Activator.CreateInstance(userDefinedType, keyspace, name, fieldNames, fieldTypes);
        }

        private static string GetTypeId(string keyspace, string name, List<string> fieldNames, List<CqlType> fieldTypes)
        {
            var builder = new StringBuilder();
            builder.Append(keyspace);
            builder.Append(",");
            builder.Append(name);
            builder.Append(",");
            for(int i = 0; i < fieldNames.Count; i++)
            {
                builder.Append(fieldNames[i]);
                builder.Append(":");
                builder.Append(fieldTypes[i].TypeName);
            }

            var typeId = builder.ToString();
            return typeId;
        }

        private Type EmitNewType(string keyspace, string name, List<string> names, List<CqlType> types)
        {

            string random = Guid.NewGuid().ToByteArray().ToHex();

            string module = string.Format("CqlSharp.UserDefined.{0}.{1}", 
                                          random, 
                                          SanitizeName(keyspace));
            
            // Get the current application domain for the current thread.
            AppDomain myCurrentDomain = AppDomain.CurrentDomain;
            var myAssemblyName = new AssemblyName { Name = module };

            // Define a dynamic assembly in the current application domain, and make sure it can be resolved
            var myAssemblyBuilder = myCurrentDomain.DefineDynamicAssembly(myAssemblyName, AssemblyBuilderAccess.Run);
            myCurrentDomain.AssemblyResolve += (sender, args) =>
            {
                if(args.Name.StartsWith(module))
                    return myAssemblyBuilder;

                return null;
            };

            // Define a dynamic module in this assembly.
            var myModuleBuilder = myAssemblyBuilder.DefineDynamicModule(module);
            
            // Define a runtime class with specified name and attributes.
            TypeBuilder myTypeBuilder = myModuleBuilder.DefineType(module+"."+SanitizeName(name), TypeAttributes.Public | TypeAttributes.Class | TypeAttributes.AutoLayout | TypeAttributes.AutoClass, typeof(object));
            
            //define fields
            for (int i = 0; i < names.Count; i++)
            {
                string fieldname = names[i];
                CqlType type = types[i];

                //define the field
                var field = myTypeBuilder.DefineField(SanitizeName(fieldname), type.Type, FieldAttributes.Public);

                //define CqlColumnAttribute
                var columnAttrParams = new[] { typeof(string) };
                var columnAttrConstructor = typeof(CqlColumnAttribute).GetConstructor(columnAttrParams);
                Debug.Assert(columnAttrConstructor != null, "Can't find CqlColumnAttribute Constructor");
                var columnOrderProperty = typeof(CqlColumnAttribute).GetProperty("Order");
                var columnAttr = new CustomAttributeBuilder(columnAttrConstructor, new object[] { fieldname }, new[] { columnOrderProperty }, new object[] { i });
                field.SetCustomAttribute(columnAttr);
            }

            //set CqlUserType attribute
            var typeAttrParams = new[] { typeof(string), typeof(string) };
            var typeAttrConstructor = typeof(CqlUserTypeAttribute).GetConstructor(typeAttrParams);
            Debug.Assert(typeAttrConstructor != null, "Can't find CqlUserTypeAttribute Constructor");
            var attr = new CustomAttributeBuilder(typeAttrConstructor, new object[] { keyspace, name });
            myTypeBuilder.SetCustomAttribute(attr);

            //set CqlTypeConverter attribute
            var converterAttrParams = new[] { typeof(Type) };
            var converterAttrConstructor = typeof(CqlTypeConverterAttribute).GetConstructor(converterAttrParams);
            Debug.Assert(converterAttrConstructor != null, "Can't find CqlTypeConverterAttribute Constructor");
            var converterType = typeof(GenericConverter<>).MakeGenericType(myTypeBuilder);
            var converterAttr = new CustomAttributeBuilder(converterAttrConstructor, new object[] { converterType });
            myTypeBuilder.SetCustomAttribute(converterAttr);

            //done, return the type
            return myTypeBuilder.CreateType();
        }

        private string SanitizeName(string name)
        {
            string nameWithSpaces = name.Replace('_', ' ').Replace('.', ' ');
            string capatilized = CultureInfo.CurrentCulture.TextInfo.ToTitleCase(nameWithSpaces);
            string nameWithNoSpaces = capatilized.Replace(" ", "");
            return nameWithNoSpaces;
        }
    }

    /// <summary>
    /// Converts Types by copying values based on column names
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class GenericConverter<T> : ITypeConverter<T>
    {
        /// <summary>
        /// Converts the source object to an object of the the given target type.
        /// </summary>
        /// <typeparam name="TTarget">The type of the target.</typeparam>
        /// <param name="source">The source.</param>
        /// <returns>an object of the the given target type</returns>
        public TTarget ConvertTo<TTarget>(T source)
        {
            return CopyValues<T, TTarget>(source);
        }

        /// <summary>
        /// Converts an object of the given source type to an instance of this converters type
        /// </summary>
        /// <typeparam name="TSource">The type of the source.</typeparam>
        /// <param name="source">The source.</param>
        /// <returns></returns>
        public T ConvertFrom<TSource>(TSource source)
        {
            return CopyValues<TSource, T>(source);
        }

        /// <summary>
        /// Copies the values from two types based CqlColumn annotations.
        /// </summary>
        /// <typeparam name="TSource">The type of the source.</typeparam>
        /// <typeparam name="TTarget">The type of the target.</typeparam>
        /// <param name="source">The source.</param>
        /// <returns></returns>
        private static TTarget CopyValues<TSource, TTarget>(TSource source)
        {
            var sourceAccessor = ObjectAccessor<TSource>.Instance;
            var targetAccessor = ObjectAccessor<TTarget>.Instance;

            var result = Activator.CreateInstance<TTarget>();

            foreach (var column in sourceAccessor.Columns)
            {
                ICqlColumnInfo<TTarget> targetColumn;
                if (targetAccessor.ColumnsByName.TryGetValue(column.Name, out targetColumn))
                    column.CopyValue(source, result, targetColumn);
            }

            return result;
        }
    }
}