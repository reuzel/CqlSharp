﻿// CqlSharp - CqlSharp
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace CqlSharp.Serialization
{
    /// <summary>
    ///   Provides access to object fields and properties based on columnn descriptions.
    /// </summary>
    internal class ObjectAccessor
    {
        /// <summary>
        ///   The translators, cached for every type
        /// </summary>
        private static readonly ConcurrentDictionary<Type, ObjectAccessor> Translators =
            new ConcurrentDictionary<Type, ObjectAccessor>();


        private readonly CqlType[] _partitionKeyTypes;
        private readonly ReadFunc[] _partitionKeyReadFuncs;


        /// <summary>
        ///   Read functions to used to read member or property values
        /// </summary>
        private readonly Dictionary<string, ReadFunc> _readFuncs;

        /// <summary>
        ///   The type for which this accessor is created
        /// </summary>
        private readonly Type _type;

        /// <summary>
        ///   Write functions to use to set fields or property values.
        /// </summary>
        private readonly Dictionary<string, WriteFunc> _writeFuncs;

        /// <summary>
        ///   Prevents a default instance of the <see cref="ObjectAccessor" /> class from being created.
        /// </summary>
        /// <param name="type"> The type. </param>
        private ObjectAccessor(Type type)
        {
            //init fields
            _writeFuncs = new Dictionary<string, WriteFunc>();
            _readFuncs = new Dictionary<string, ReadFunc>();
            _type = type;

            var keyMembers = new List<Tuple<int, ReadFunc, CqlType>>();

            //set default keyspace and table name to empty strings (nothing)
            string keyspace = "";
            string table = "";

            //set default table name to class name if table is not anonymous
            if (!type.IsAnonymous())
                table = type.Name;

            //check for CqlTable attribute
            var tableAttribute = Attribute.GetCustomAttribute(type, typeof(CqlTableAttribute)) as CqlTableAttribute;
            if (tableAttribute != null)
            {
                //overwrite keyspace if any
                keyspace = tableAttribute.Keyspace ?? keyspace;

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
                    _writeFuncs[name] = prop.SetValue;
                }

                //add the read func if we can read the property
                if (prop.CanRead && !prop.GetMethod.IsPrivate)
                {
                    _readFuncs[name] = prop.GetValue;
                    SetPartitionKeyMember(keyMembers, prop, prop.GetValue);
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
                if (!field.IsInitOnly) _writeFuncs[name] = field.SetValue;

                _readFuncs[name] = field.GetValue;
                SetPartitionKeyMember(keyMembers, field, field.GetValue);
            }

            //sort keyMembers on partitionIndex
            keyMembers.Sort((a, b) => a.Item1.CompareTo(b.Item1));
            _partitionKeyReadFuncs = keyMembers.Select(km => km.Item2).ToArray();
            _partitionKeyTypes = keyMembers.Select(km => km.Item3).ToArray();

        }

        /// <summary>
        /// Sets the partition key member.
        /// </summary>
        /// <param name="member">The member.</param>
        /// <param name="reader">The reader.</param>
        /// <exception cref="System.ArgumentException">CqlType must be set on ColumnAttribute if PartitionKeyIndex is set.</exception>
        private void SetPartitionKeyMember(List<Tuple<int, ReadFunc, CqlType>> keyMembers, MemberInfo member, ReadFunc reader)
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
                    keyMembers.Add(new Tuple<int, ReadFunc, CqlType>(columnAttribute.PartitionKeyIndex, reader, columnAttribute.CqlType));
                }
            }
        }

        /// <summary>
        ///   Gets the accessor for type T.
        /// </summary>
        /// <typeparam name="T"> type of the object accessed </typeparam>
        /// <returns> Accessor for objects of type T </returns>
        public static ObjectAccessor GetAccessor<T>()
        {
            Type type = typeof(T);
            return Translators.GetOrAdd(type, (t) => new ObjectAccessor(t));
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
            string cName;

            //check for ignore attribute
            var ignoreAttribute =
                Attribute.GetCustomAttribute(member, typeof(CqlIgnoreAttribute)) as CqlIgnoreAttribute;

            //return null if ignore attribute is set
            if (ignoreAttribute != null)
                return null;

            //check for column attribute
            var columnAttribute =
                Attribute.GetCustomAttribute(member, typeof(CqlColumnAttribute)) as CqlColumnAttribute;

            if (columnAttribute != null)
            {
                //set column name, table and keyspace based on attribute info
                cName = columnAttribute.Column;
            }
            else
            {
                //set column name, table and keyspace info based on property name
                cName = member.Name;
            }

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
        public bool TryGetValue(CqlColumn column, object source, out object value)
        {
            if (column == null)
                throw new ArgumentNullException("column");

            if (source == null)
                throw new ArgumentNullException("source");

            if (source.GetType() != _type)
                throw new ArgumentException("Source is not of the correct type!", "source");

            ReadFunc func;

            string name = column.Keyspace + "." + column.Table + "." + column.Name;
            if (_readFuncs.TryGetValue(name.ToLower(), out func))
            {
                value = func(source);
                return true;
            }

            name = "." + column.Table + "." + column.Name;
            if (_readFuncs.TryGetValue(name.ToLower(), out func))
            {
                value = func(source);
                return true;
            }

            name = ".." + column.Name;
            if (_readFuncs.TryGetValue(name.ToLower(), out func))
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
        public bool TrySetValue(CqlColumn column, object target, object value)
        {
            if (column == null)
                throw new ArgumentNullException("column");

            if (target == null)
                throw new ArgumentNullException("target");

            if (target.GetType() != _type)
                throw new ArgumentException("Source is not of the correct type!", "target");

            WriteFunc func;

            string name = column.Keyspace + "." + column.Table + "." + column.Name;
            if (_writeFuncs.TryGetValue(name.ToLower(), out func))
            {
                func(target, value);
                return true;
            }

            name = "." + column.Table + "." + column.Name;
            if (_writeFuncs.TryGetValue(name.ToLower(), out func))
            {
                func(target, value);
                return true;
            }

            name = ".." + column.Name;
            if (_writeFuncs.TryGetValue(name.ToLower(), out func))
            {
                func(target, value);
                return true;
            }

            return false;
        }

        public void SetPartitionKey(PartitionKey key, object value)
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

        #region Nested type: ReadFunc

        /// <summary>
        ///   Read delagate, usable to read fields or properties
        /// </summary>
        /// <param name="target"> The target. </param>
        /// <returns> </returns>
        private delegate object ReadFunc(Object target);

        #endregion

        #region Nested type: WriteFunc

        /// <summary>
        ///   Write delegate, usable to write fields or properties
        /// </summary>
        /// <param name="target"> The target. </param>
        /// <param name="value"> The value. </param>
        private delegate void WriteFunc(Object target, object value);

        #endregion
    }
}