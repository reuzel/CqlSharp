// CqlSharp - CqlSharp
// Copyright (c) 2013 Joost Reuzel
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

using CqlSharp.Network.Partition;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace CqlSharp.Serialization
{
    /// <summary>
    ///   Provides access to object fields and properties based on columnn descriptions.
    /// </summary>
    internal class ObjectAccessor<T>
    {
        /// <summary>
        /// SingleTon instance
        /// </summary>
        public static readonly ObjectAccessor<T> Instance = new ObjectAccessor<T>();

        private readonly Func<T, object>[] _partitionKeyReadFuncs;
        private readonly CqlType[] _partitionKeyTypes;
        private readonly bool _keySpaceSet;
        private readonly bool _tableSet;

        /// <summary>
        ///   Read functions to used to read member or property values
        /// </summary>
        private readonly Dictionary<string, Func<T, object>> _readFuncs;

        /// <summary>
        ///   Write functions to use to set fields or property values.
        /// </summary>
        private readonly Dictionary<string, Action<T, object>> _writeFuncs;

        /// <summary>
        /// Prevents a default instance of the <see cref="ObjectAccessor{T}" /> class from being created.
        /// </summary>
        private ObjectAccessor()
        {
            //init fields
            _writeFuncs = new Dictionary<string, Action<T, object>>();
            _readFuncs = new Dictionary<string, Func<T, object>>();

            var keyMembers = new List<Tuple<int, Func<T, object>, CqlType>>();

            //set default keyspace and table name to empty strings (nothing)
            string keyspace = "";
            string table = "";

            //set default table name to class name if table is not anonymous
            Type type = typeof(T);
            _tableSet = !type.IsAnonymous();
            if (_tableSet)
                table = type.Name;

            //check for CqlTable attribute
            var tableAttribute = Attribute.GetCustomAttribute(type, typeof(CqlTableAttribute)) as CqlTableAttribute;
            if (tableAttribute != null)
            {
                //overwrite keyspace if any
                _keySpaceSet = tableAttribute.Keyspace != null;
                if (_keySpaceSet)
                    keyspace = tableAttribute.Keyspace;

                //set default table name
                table = tableAttribute.Table ?? table;
            }

            //go over all properties
            foreach (PropertyInfo prop in type.GetProperties())
            {
                //get the column name of the property
                string name = GetColumnName(prop, table, keyspace);

                //check if we get a proper name
                if (string.IsNullOrEmpty(name))
                    continue;

                //add write func if we can write the property
                if (prop.CanWrite && !prop.SetMethod.IsPrivate)
                {
                    _writeFuncs[name] = MakeSetterDelegate(prop);
                }

                //add the read func if we can read the property
                if (prop.CanRead && !prop.GetMethod.IsPrivate)
                {
                    _readFuncs[name] = MakeGetterDelegate(prop);
                    SetPartitionKeyMember(keyMembers, prop, _readFuncs[name]);
                }
            }

            //go over all fields
            foreach (FieldInfo field in type.GetFields())
            {
                //get the column name of the field
                string name = GetColumnName(field, table, keyspace);

                //check if we get a proper name
                if (string.IsNullOrEmpty(name))
                    continue;


                //set getter and setter functions
                if (!field.IsInitOnly)
                {
                    _writeFuncs[name] = MakeFieldSetterDelegate(field);
                }

                _readFuncs[name] = MakeFieldGetterDelegate(field);
                SetPartitionKeyMember(keyMembers, field, _readFuncs[name]);
            }

            //sort keyMembers on partitionIndex
            keyMembers.Sort((a, b) => a.Item1.CompareTo(b.Item1));
            _partitionKeyReadFuncs = keyMembers.Select(km => km.Item2).ToArray();
            _partitionKeyTypes = keyMembers.Select(km => km.Item3).ToArray();
        }

        /// <summary>
        /// Sets the partition key member.
        /// </summary>
        /// <param name="keyMembers">The key members.</param>
        /// <param name="member">The member.</param>
        /// <param name="reader">The reader.</param>
        /// <exception cref="System.ArgumentException">CqlType must be set on ColumnAttribute if PartitionKeyIndex is set.</exception>
        private void SetPartitionKeyMember(List<Tuple<int, Func<T, object>, CqlType>> keyMembers, MemberInfo member, Func<T, object> reader)
        {
            //check for column attribute
            var columnAttribute =
                Attribute.GetCustomAttribute(member, typeof(CqlColumnAttribute)) as CqlColumnAttribute;

            if (columnAttribute != null)
            {
                if (columnAttribute.PartitionKeyIndex >= 0)
                {
                    //if (!columnAttribute.CqlType.HasValue)
                    //    throw new ArgumentException("CqlType must be set on ColumnAttribute if PartitionKeyIndex is set.");

                    //add the member
                    keyMembers.Add(new Tuple<int, Func<T, object>, CqlType>(columnAttribute.PartitionKeyIndex, reader,
                                                                     columnAttribute.CqlType));
                }
            }
        }

        /// <summary>
        ///   Gets the name of the column of the specified member.
        /// </summary>
        /// <param name="member"> The member. </param>
        /// <param name="table"> The table. </param>
        /// <param name="keyspace"> The keyspace. </param>
        /// <returns> </returns>
        private static string GetColumnName(MemberInfo member, string table, string keyspace)
        {
            //check for ignore attribute
            var ignoreAttribute =
                Attribute.GetCustomAttribute(member, typeof(CqlIgnoreAttribute)) as CqlIgnoreAttribute;

            //return null if ignore attribute is set
            if (ignoreAttribute != null)
                return null;

            //check for column attribute
            var columnAttribute =
                Attribute.GetCustomAttribute(member, typeof(CqlColumnAttribute)) as CqlColumnAttribute;

            string cName = columnAttribute != null ? columnAttribute.Column : member.Name;

            return (keyspace + "." + table + "." + cName).ToLower();
        }

        /// <summary>
        ///   Tries to get a value from the source, based on the column description
        /// </summary>
        /// <param name="column"> The column. </param>
        /// <param name="source"> The source. </param>
        /// <param name="value"> The value. </param>
        /// <returns> true, if the value could be distilled from the source </returns>
        /// <exception cref="System.ArgumentNullException">column</exception>
        /// <exception cref="System.ArgumentException">Source is not of the correct type!;source</exception>
        public bool TryGetValue(CqlColumn column, T source, out object value)
        {
            if (column == null)
                throw new ArgumentNullException("column");

            // ReSharper disable CompareNonConstrainedGenericWithNull
            if (source == null)
                // ReSharper restore CompareNonConstrainedGenericWithNull
                throw new ArgumentNullException("source");

            if (source.GetType() != typeof(T))
                throw new ArgumentException("Source is not of the correct type!", "source");

            Func<T, object> func;

            if (_keySpaceSet && _tableSet)
            {
                if (_readFuncs.TryGetValue(column.KsTableNameNormalized, out func))
                {
                    value = func(source);
                    return true;
                }
            }
            else if (_tableSet)
            {
                if (_readFuncs.TryGetValue(column.TableNameNormalized, out func))
                {
                    value = func(source);
                    return true;
                }
            }
            else if (_readFuncs.TryGetValue(column.NameNormalized, out func))
            {
                value = func(source);
                return true;
            }

            value = null;
            return false;
        }

        /// <summary>
        ///   Tries to set a property or field of the specified object, based on the column description
        /// </summary>
        /// <param name="column"> The column. </param>
        /// <param name="target"> The target. </param>
        /// <param name="value"> The value. </param>
        /// <returns> true if the property or field value is set </returns>
        /// <exception cref="System.ArgumentNullException">column</exception>
        /// <exception cref="System.ArgumentException">Source is not of the correct type!;target</exception>
        public bool TrySetValue(CqlColumn column, T target, object value)
        {
            Action<T, object> func;

            if (_keySpaceSet && _tableSet)
            {
                if (_writeFuncs.TryGetValue(column.KsTableNameNormalized, out func))
                {
                    func(target, value);
                    return true;
                }
            }
            else if (_tableSet)
            {
                if (_writeFuncs.TryGetValue(column.TableNameNormalized, out func))
                {
                    func(target, value);

                    return true;
                }
            }
            else if (_writeFuncs.TryGetValue(column.NameNormalized, out func))
            {
                func(target, value);

                return true;
            }

            return false;
        }

        public void SetPartitionKey(PartitionKey key, T value)
        {
            int length = _partitionKeyReadFuncs.Length;
            if (length > 0)
            {
                var values = new object[length];
                for (int i = 0; i < length; i++)
                {
                    values[i] = _partitionKeyReadFuncs[i](value);
                }

                key.Set(_partitionKeyTypes, values);
            }
        }


        static Func<T, object> MakeGetterDelegate(PropertyInfo property)
        {
            MethodInfo getMethod = property.GetGetMethod();
            var target = Expression.Parameter(typeof(T));
            var body = Expression.Convert(Expression.Call(target, getMethod), typeof(object));
            return Expression.Lambda<Func<T, object>>(body, target)
                .Compile();
        }

        static Action<T, object> MakeSetterDelegate(PropertyInfo property)
        {
            MethodInfo setMethod = property.GetSetMethod();
            var target = Expression.Parameter(typeof(T));
            var value = Expression.Parameter(typeof(object));
            var valueOrDefault = Expression.Condition(
                Expression.Equal(value, Expression.Constant(null)),
                Expression.Default(property.PropertyType),
                Expression.Convert(value, property.PropertyType));
            var body = Expression.Call(target, setMethod, valueOrDefault);
            return Expression.Lambda<Action<T, object>>(body, target, value)
                .Compile();
        }

        static Func<T, object> MakeFieldGetterDelegate(FieldInfo property)
        {
            var target = Expression.Parameter(typeof(T));
            var body = Expression.Convert(Expression.Field(target, property), typeof(object));
            return Expression.Lambda<Func<T, object>>(body, target).Compile();
        }

        static Action<T, object> MakeFieldSetterDelegate(FieldInfo property)
        {
            var target = Expression.Parameter(typeof(T));
            var field = Expression.Field(target, property);
            var value = Expression.Parameter(typeof(object));
            var valueOrDefault = Expression.Condition(
                Expression.Equal(value, Expression.Constant(null)),
                Expression.Default(property.FieldType),
                Expression.Convert(value, property.FieldType));
            var body = Expression.Assign(field, valueOrDefault);
            return Expression.Lambda<Action<T, object>>(body, target, value).Compile();
        }
    }



}