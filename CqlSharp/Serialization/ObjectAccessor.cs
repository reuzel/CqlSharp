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

using CqlSharp.Network.Partition;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace CqlSharp.Serialization
{
    /// <summary>
    ///   Provides access to object fields and properties based on columnn descriptions.
    /// </summary>
    /// <typeparam name="T"> </typeparam>
    public class ObjectAccessor<T>
    {
        /// <summary>
        ///   Singleton instance
        /// </summary>
        public static readonly ObjectAccessor<T> Instance = new ObjectAccessor<T>();

        private readonly ReadOnlyCollection<CqlColumnInfo<T>> _clusteringKeys;

        private readonly ReadOnlyCollection<CqlColumnInfo<T>> _columns;
        private readonly ReadOnlyDictionary<MemberInfo, CqlColumnInfo<T>> _columnsByMember;
        private readonly ReadOnlyDictionary<string, CqlColumnInfo<T>> _columnsByName;
        private readonly ReadOnlyCollection<CqlColumnInfo<T>> _normalColumns;
        private readonly CqlType[] _partitionKeyTypes;
        private readonly ReadOnlyCollection<CqlColumnInfo<T>> _partitionKeys;
        private readonly Type _type;

        /// <summary>
        ///   Prevents a default instance of the <see cref="ObjectAccessor{T}" /> class from being created.
        /// </summary>
        private ObjectAccessor()
        {
            //get table and keyspace name
            SetTableProperties();

            //get type
            _type = typeof(T);

            //create a column List
            var columns = new List<CqlColumnInfo<T>>();

            //go over all properties
            foreach (PropertyInfo prop in _type.GetProperties())
            {
                if (ShouldIgnoreMember(prop))
                    continue;

                //create the column info object
                CqlColumnInfo<T> info = CreateColumnInfo(prop);

                //set the type
                info.Type = prop.PropertyType;

                //add the read func if we can read the property
                if (prop.CanRead && !prop.GetMethod.IsPrivate)
                {
                    info.ReadFunction = MakeGetterDelegate(prop);
                }

                //add write func if we can write the property
                if (prop.CanWrite && !prop.SetMethod.IsPrivate)
                {
                    info.WriteFunction = MakeSetterDelegate(prop);
                }

                columns.Add(info);
            }

            //go over all fields
            foreach (FieldInfo field in _type.GetFields())
            {
                if (ShouldIgnoreMember(field))
                    continue;

                //create the column info object
                CqlColumnInfo<T> info = CreateColumnInfo(field);

                //set the type
                info.Type = field.FieldType;

                //set getter functions
                info.ReadFunction = MakeFieldGetterDelegate(field);

                //set setter functions if not readonly
                if (!field.IsInitOnly)
                {
                    info.WriteFunction = MakeFieldSetterDelegate(field);
                }

                columns.Add(info);
            }
            _columns = new ReadOnlyCollection<CqlColumnInfo<T>>(columns);

            //fill index by name
            var columnsByName = new Dictionary<string, CqlColumnInfo<T>>();
            foreach (var column in _columns)
            {
                var name = column.Name;
                columnsByName[name] = column;
                if (IsTableSet)
                {
                    columnsByName[Table + "." + name] = column;
                    if (IsKeySpaceSet)
                        columnsByName[Keyspace + "." + Table + "." + name] = column;
                }
            }
            _columnsByName = new ReadOnlyDictionary<string, CqlColumnInfo<T>>(columnsByName);

            //fill index by member
            _columnsByMember =
                new ReadOnlyDictionary<MemberInfo, CqlColumnInfo<T>>(_columns.ToDictionary(column => column.MemberInfo));

            //column (sub)sets
            _partitionKeys =
                new ReadOnlyCollection<CqlColumnInfo<T>>(
                    _columns.Where(column => column.IsPartitionKey).OrderBy(column => column.Order).ToList());
            _partitionKeyTypes = _partitionKeys.Select(info => info.CqlType).ToArray();
            _clusteringKeys =
                new ReadOnlyCollection<CqlColumnInfo<T>>(
                    _columns.Where(column => column.IsClusteringKey).OrderBy(column => column.Order).ToList());
            _normalColumns =
                new ReadOnlyCollection<CqlColumnInfo<T>>(
                    _columns.Where(column => !column.IsClusteringKey && !column.IsPartitionKey).ToList());
        }

        /// <summary>
        ///   Gets a value indicating whether [is key space set].
        /// </summary>
        /// <value> <c>true</c> if [is key space set]; otherwise, <c>false</c> . </value>
        public bool IsKeySpaceSet { get; private set; }

        /// <summary>
        ///   Gets the keyspace.
        /// </summary>
        /// <value> The keyspace. </value>
        public string Keyspace { get; private set; }

        /// <summary>
        ///   Gets a value indicating whether [is table set].
        /// </summary>
        /// <value> <c>true</c> if [is table set]; otherwise, <c>false</c> . </value>
        public bool IsTableSet { get; private set; }

        /// <summary>
        ///   Gets the table name.
        /// </summary>
        /// <value> The table. </value>
        public string Table { get; private set; }


        /// <summary>
        ///   Gets the type this accessor can handle
        /// </summary>
        /// <value> The type. </value>
        public Type Type
        {
            get { return _type; }
        }

        /// <summary>
        ///   Gets the partition keys.
        /// </summary>
        /// <value> The partition keys. </value>
        public ReadOnlyCollection<CqlColumnInfo<T>> PartitionKeys
        {
            get { return _partitionKeys; }
        }

        /// <summary>
        ///   Gets the clustering keys.
        /// </summary>
        /// <value> The clustering keys. </value>
        public ReadOnlyCollection<CqlColumnInfo<T>> ClusteringKeys
        {
            get { return _clusteringKeys; }
        }

        /// <summary>
        ///   Gets the normal (non-key) columns.
        /// </summary>
        /// <value> The normal columns. </value>
        public ReadOnlyCollection<CqlColumnInfo<T>> NormalColumns
        {
            get { return _normalColumns; }
        }

        /// <summary>
        ///   Gets all the columns.
        /// </summary>
        /// <value> The columns. </value>
        public ReadOnlyCollection<CqlColumnInfo<T>> Columns
        {
            get { return _columns; }
        }

        /// <summary>
        ///   Gets the columns by field or property member.
        /// </summary>
        /// <value> The columns by member. </value>
        public ReadOnlyDictionary<MemberInfo, CqlColumnInfo<T>> ColumnsByMember
        {
            get { return _columnsByMember; }
        }

        /// <summary>
        ///   Gets the columns by column name. When the Table or Keyspace is known the dictionary
        ///   will contain entries where the column name is combined with the Table or Keyspace names.
        /// </summary>
        /// <value> The columns by member. </value>
        public ReadOnlyDictionary<string, CqlColumnInfo<T>> ColumnsByName
        {
            get { return _columnsByName; }
        }

        /// <summary>
        ///   Sets the table properties.
        /// </summary>
        private void SetTableProperties()
        {
            //set default keyspace and table name to empty strings (nothing)
            Keyspace = null;
            Table = null;

            //set default table name to class name if table is not anonymous
            Type type = typeof(T);
            IsTableSet = !type.IsAnonymous();
            if (IsTableSet)
                Table = type.Name.ToLower();

            //check for CqlTable attribute
            var tableAttribute = Attribute.GetCustomAttribute(type, typeof(CqlTableAttribute)) as CqlTableAttribute;
            if (tableAttribute != null)
            {
                //overwrite keyspace if any
                IsKeySpaceSet = tableAttribute.Keyspace != null;
                if (IsKeySpaceSet)
                    Keyspace = tableAttribute.Keyspace;

                //set default table name
                Table = tableAttribute.Table ?? Table;
            }
        }

        /// <summary>
        ///   Checks wether the member must be ignored
        /// </summary>
        /// <param name="member"> The member. </param>
        /// <returns> </returns>
        private static bool ShouldIgnoreMember(MemberInfo member)
        {
            //check for ignore attribute
            var ignoreAttribute =
                Attribute.GetCustomAttribute(member, typeof(CqlIgnoreAttribute)) as CqlIgnoreAttribute;

            //return true if ignore attribute is set
            return ignoreAttribute != null;
        }

        /// <summary>
        ///   Creates the column information.
        /// </summary>
        /// <param name="prop"> The property. </param>
        /// <returns> </returns>
        private static CqlColumnInfo<T> CreateColumnInfo(MemberInfo prop)
        {
            //create new info for property
            var info = new CqlColumnInfo<T> { MemberInfo = prop };

            //get the column name and type of the property
            SetColumnInfo(prop, info);

            //set key info if any
            SetKeyInfo(prop, info);

            //set index info if any
            SetIndexInfo(prop, info);

            return info;
        }

        /// <summary>
        ///   Sets any index information
        /// </summary>
        /// <param name="member"> The member. </param>
        /// <param name="column"> The column. </param>
        private static void SetIndexInfo(MemberInfo member, CqlColumnInfo<T> column)
        {
            //check for column attribute
            var indexAttribute =
                Attribute.GetCustomAttribute(member, typeof(CqlIndexAttribute)) as CqlIndexAttribute;

            if (indexAttribute != null)
            {
                column.IsIndexed = true;
                column.IndexName = indexAttribute.Name;
            }
            else
            {
                column.IsIndexed = false;
            }
        }

        /// <summary>
        ///   Sets the key information.
        /// </summary>
        /// <param name="member"> The member. </param>
        /// <param name="column"> The column. </param>
        private static void SetKeyInfo(MemberInfo member, CqlColumnInfo<T> column)
        {
            //check for column attribute
            var keyAttribute =
                Attribute.GetCustomAttribute(member, typeof(CqlKeyAttribute)) as CqlKeyAttribute;

            if (keyAttribute != null)
            {
                column.Order = keyAttribute.Order;
                column.IsPartitionKey = keyAttribute.IsPartitionKey;
                column.IsClusteringKey = !keyAttribute.IsPartitionKey;
            }
            else
            {
                column.IsPartitionKey = false;
                column.IsClusteringKey = false;
            }
        }

        /// <summary>
        ///   Gets the name of the column of the specified member.
        /// </summary>
        /// <param name="member"> The member. </param>
        /// <param name="column"> the column. </param>
        private static void SetColumnInfo(MemberInfo member, CqlColumnInfo<T> column)
        {
            //check for column attribute
            var columnAttribute =
                Attribute.GetCustomAttribute(member, typeof(CqlColumnAttribute)) as CqlColumnAttribute;

            //get column name from attribute or base on name otherwise
            if (columnAttribute != null && columnAttribute.Column != null)
            {
                column.Name = columnAttribute.Column;
            }
            else
            {
                column.Name = member.Name.ToLower();
            }

            //get CqlType from attribute (if any)
            if (columnAttribute != null && columnAttribute.CqlType.HasValue)
            {
                column.CqlType = columnAttribute.CqlType.Value;
                return;
            }

            //distill CqlType from property
            var prop = member as PropertyInfo;
            if (prop != null)
            {
                column.CqlType = prop.PropertyType.ToCqlType();
                return;
            }

            //distill CqlType from field
            var field = member as FieldInfo;
            if (field != null)
            {
                column.CqlType = field.FieldType.ToCqlType();
                return;
            }

            throw new CqlException("Only fields or properties are allowed as columns");
        }

        /// <summary>
        ///   Makes the getter delegate.
        /// </summary>
        /// <param name="property"> The property. </param>
        /// <returns> </returns>
        private static Func<T, object> MakeGetterDelegate(PropertyInfo property)
        {
            MethodInfo getMethod = property.GetGetMethod();
            var target = Expression.Parameter(typeof(T));
            var body = Expression.Convert(Expression.Call(target, getMethod), typeof(object));
            return Expression.Lambda<Func<T, object>>(body, target)
                .Compile();
        }

        /// <summary>
        ///   Makes the setter delegate.
        /// </summary>
        /// <param name="property"> The property. </param>
        /// <returns> </returns>
        private static Action<T, object> MakeSetterDelegate(PropertyInfo property)
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

        /// <summary>
        ///   Makes the field getter delegate.
        /// </summary>
        /// <param name="property"> The property. </param>
        /// <returns> </returns>
        private static Func<T, object> MakeFieldGetterDelegate(FieldInfo property)
        {
            var target = Expression.Parameter(typeof(T));
            var body = Expression.Convert(Expression.Field(target, property), typeof(object));
            return Expression.Lambda<Func<T, object>>(body, target).Compile();
        }

        /// <summary>
        ///   Makes the field setter delegate.
        /// </summary>
        /// <param name="property"> The property. </param>
        /// <returns> </returns>
        private static Action<T, object> MakeFieldSetterDelegate(FieldInfo property)
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

        /// <summary>
        ///   Tries to get a value from the source, based on the column description
        /// </summary>
        /// <param name="columnName"> Name of the column. </param>
        /// <param name="source"> The source. </param>
        /// <param name="value"> The value. </param>
        /// <returns> true, if the value could be distilled from the source </returns>
        /// <exception cref="System.ArgumentNullException">columnName or source are null</exception>
        public bool TryGetValue(string columnName, T source, out object value)
        {
            if (columnName == null)
                throw new ArgumentNullException("columnName");

            // ReSharper disable CompareNonConstrainedGenericWithNull
            if (source == null)
                // ReSharper restore CompareNonConstrainedGenericWithNull
                throw new ArgumentNullException("source");

            CqlColumnInfo<T> column;
            if (_columnsByName.TryGetValue(columnName, out column))
            {
                Func<T, object> func = column.ReadFunction;
                if (func != null)
                {
                    value = func(source);
                    return true;
                }
            }

            value = null;
            return false;
        }

        /// <summary>
        ///   Tries to set a property or field of the specified object, based on the column description
        /// </summary>
        /// <param name="columnName"> Name of the column. </param>
        /// <param name="target"> The target. </param>
        /// <param name="value"> The value. </param>
        /// <returns> true if the property or field value is set </returns>
        /// <exception cref="System.ArgumentNullException">columnName or target are null</exception>
        public bool TrySetValue(string columnName, T target, object value)
        {
            if (columnName == null)
                throw new ArgumentNullException("columnName");

            // ReSharper disable CompareNonConstrainedGenericWithNull
            if (target == null)
                // ReSharper restore CompareNonConstrainedGenericWithNull
                throw new ArgumentNullException("target");


            CqlColumnInfo<T> column;
            if (_columnsByName.TryGetValue(columnName, out column))
            {
                Action<T, object> func = column.WriteFunction;
                if (func != null)
                {
                    func(target, value);
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        ///   Sets the partition key based on the data found in a table entry.
        /// </summary>
        /// <param name="key"> The key. </param>
        /// <param name="value"> The value. </param>
        /// <exception cref="CqlException">Unable to read value for partition key column  + _partitionKeys[i].Name</exception>
        public void SetPartitionKey(PartitionKey key, T value)
        {
            int length = _partitionKeys.Count;
            if (length > 0)
            {
                var values = new object[length];
                for (int i = 0; i < length; i++)
                {
                    if (_partitionKeys[i].ReadFunction == null)
                        throw new CqlException("Unable to read value for partition key column " + _partitionKeys[i].Name);

                    values[i] = _partitionKeys[i].ReadFunction(value);
                }

                key.Set(_partitionKeyTypes, values);
            }
        }
    }
}