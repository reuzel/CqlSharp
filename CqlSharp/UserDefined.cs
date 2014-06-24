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
using System.Dynamic;
using CqlSharp.Serialization;
using CqlSharp.Serialization.Marshal;

namespace CqlSharp
{
    /// <summary>
    /// Represents an instance of a User Defined Type. Typically this object
    /// is not used directly but it is cast to a type holds each field as
    /// a seperated property.
    /// </summary>
    [CqlTypeConverter(typeof(UserDefinedConverter))]
    public class UserDefined : DynamicObject
    {
        public UserDefinedType Type { get; private set; }
        private object[] _values;

        /// <summary>
        /// Gets or sets the values.
        /// </summary>
        /// <value>
        /// The values.
        /// </value>
        /// <exception cref="System.ArgumentException">
        /// Number of values provided does not match the number of fields in the
        /// UserDefinedType
        /// </exception>
        public object[] Values
        {
            get { return _values; }

            set
            {
                if(value.Length != Type.GetFieldCount())
                {
                    throw new ArgumentException(
                        "Number of values provided does not match the number of fields in the UserDefinedType");
                }

                _values = value;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="UserDefined" /> class.
        /// </summary>
        /// <param name="userDefinedType">Type of the user defined.</param>
        /// <param name="values">The values.</param>
        /// <exception cref="System.ArgumentException">
        /// Number of values provided does not match the number of fields in the
        /// UserDefinedType;values
        /// </exception>
        public UserDefined(UserDefinedType userDefinedType, object[] values)
        {
            Type = userDefinedType;

            if(values.Length != Type.GetFieldCount())
            {
                throw new ArgumentException(
                    "Number of values provided does not match the number of fields in the UserDefinedType", "values");
            }

            _values = values;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="UserDefined" /> class.
        /// </summary>
        /// <param name="cqlType">Type of the CQL.</param>
        public UserDefined(UserDefinedType cqlType)
        {
            Type = cqlType;
            _values = new object[Type.GetFieldCount()];
        }

        /// <summary>
        /// Gets or sets the value with the specified name.
        /// </summary>
        /// <value>
        /// The <see cref="System.Object" />.
        /// </value>
        /// <param name="name">The name.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentOutOfRangeException">
        /// name;UserDefined object does not contain a field with name  +
        /// name
        /// </exception>
        public object this[string name]
        {
            get
            {
                int index = Type.GetFieldIndex(name);

                if(index < 0)
                {
                    throw new ArgumentOutOfRangeException("name",
                                                          "UserDefined object does not contain a field with name " +
                                                          name);
                }

                return _values[index];
            }

            set
            {
                int index = Type.GetFieldIndex(name);

                if(index < 0)
                {
                    throw new ArgumentOutOfRangeException("name",
                                                          "UserDefined object does not contain a field with name " +
                                                          name);
                }

                _values[index] = value;
            }
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            string name = binder.Name;
            int index = Type.GetFieldIndex(name);
            if(index < 0)
            {
                result = null;
                return false;
            }

            result = Converter.ChangeType(_values[index], binder.ReturnType);
            return true;
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            string name = binder.Name;
            int index = Type.GetFieldIndex(name);
            if(index < 0)
                return false;

            _values[index] = Converter.ChangeType(value, Type.GetFieldType(index).Type);
            return true;
        }

        #region Type converter

        /// <summary>
        /// Defines type conversions for UserDefined objects
        /// </summary>
        private class UserDefinedConverter : ITypeConverter<UserDefined>
        {
            /// <summary>
            /// Converts the source object to an object of the the given target type.
            /// </summary>
            /// <typeparam name="TTarget">The type of the target.</typeparam>
            /// <param name="source">The source.</param>
            /// <returns>an object of the the given target type</returns>
            public TTarget ConvertTo<TTarget>(UserDefined source)
            {
                var instance = Activator.CreateInstance<TTarget>();
                var accessor = ObjectAccessor<TTarget>.Instance;

                int count = source.Type.GetFieldCount();
                for(int i = 0; i < count; i++)
                {
                    ICqlColumnInfo<TTarget> column;
                    if(accessor.ColumnsByName.TryGetValue(source.Type.GetFieldName(i), out column))
                    {
                        object value = Converter.ChangeType(source.Values[i], column.Type);
                        column.Write(instance, value);
                    }
                }

                return instance;
            }

            /// <summary>
            /// Converts an object of the given source type to an instance of this converters type
            /// </summary>
            /// <typeparam name="TSource">The type of the source.</typeparam>
            /// <param name="source">The source.</param>
            /// <returns></returns>
            public UserDefined ConvertFrom<TSource>(TSource source)
            {
                var type = CqlType.CreateType(typeof(TSource)) as UserDefinedType;

                if(type == null)
                    throw new ArgumentException("Source must be mapped to a UserDefinedType to have it converted");

                var accessor = ObjectAccessor<TSource>.Instance;

                int count = type.GetFieldCount();
                var values = new object[count];

                for(int i = 0; i < count; i++)
                {
                    ICqlColumnInfo<TSource> column;
                    if(accessor.ColumnsByName.TryGetValue(type.GetFieldName(i), out column))
                        values[i] = column.Read<object>(source);
                }

                return new UserDefined(type, values);
            }
        }

        #endregion

    }
}