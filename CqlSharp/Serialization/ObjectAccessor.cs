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
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;
using CqlSharp.Network.Partition;

namespace CqlSharp.Serialization
{
    /// <summary>
    /// Provides access to object fields and properties based on columnn descriptions.
    /// </summary>
    /// <typeparam name="T"> </typeparam>
    public class ObjectAccessor<T> : IObjectAccessor
    {
        /// <summary>
        /// Singleton instance
        /// </summary>
        public static readonly ObjectAccessor<T> Instance = new ObjectAccessor<T>();

        private readonly ReadOnlyCollection<ICqlColumnInfo<T>> _clusteringKeys;
        private readonly ReadOnlyCollection<ICqlColumnInfo<T>> _columns;
        private readonly ReadOnlyDictionary<MemberInfo, ICqlColumnInfo<T>> _columnsByMember;
        private readonly ReadOnlyDictionary<string, ICqlColumnInfo<T>> _columnsByName;
        private readonly ReadOnlyCollection<ICqlColumnInfo<T>> _normalColumns;
        private readonly CqlType[] _partitionKeyTypes;
        private readonly ReadOnlyCollection<ICqlColumnInfo<T>> _partitionKeys;
        private Type _type;

        //non-generic version of the collections
        private readonly ReadOnlyCollection<ICqlColumnInfo> _clusteringKeysNG;
        private readonly ReadOnlyCollection<ICqlColumnInfo> _columnsNG;
        private readonly ReadOnlyDictionary<MemberInfo, ICqlColumnInfo> _columnsByMemberNG;
        private readonly ReadOnlyDictionary<string, ICqlColumnInfo> _columnsByNameNG;
        private readonly ReadOnlyCollection<ICqlColumnInfo> _normalColumnsNG;
        private readonly ReadOnlyCollection<ICqlColumnInfo> _partitionKeysNG;

        /// <summary>
        /// Prevents a default instance of the <see cref="ObjectAccessor{T}" /> class from being created.
        /// </summary>
        private ObjectAccessor()
        {
            //get table and keyspace name
            SetEntityProperties();

            //get columns
            var columns = GetColumns();
            SetKeyInfo(columns);
            _columns = new ReadOnlyCollection<ICqlColumnInfo<T>>(columns);
            _columnsNG = new ReadOnlyCollection<ICqlColumnInfo>(columns);

            //fill index by name
            var columnsByName = new Dictionary<string, ICqlColumnInfo<T>>();
            foreach(var column in _columns)
            {
                var name = column.Name;
                columnsByName[name] = column;
                if(IsNameSet)
                {
                    columnsByName[Name + "." + name] = column;
                    if(IsKeySpaceSet)
                        columnsByName[Keyspace + "." + Name + "." + name] = column;
                }
            }
            _columnsByName = new ReadOnlyDictionary<string, ICqlColumnInfo<T>>(columnsByName);
            _columnsByNameNG =
                new ReadOnlyDictionary<string, ICqlColumnInfo>(columnsByName.ToDictionary(kvp => kvp.Key,
                                                                                          kvp =>
                                                                                              (ICqlColumnInfo)kvp.Value));

            //fill index by member
            _columnsByMember =
                new ReadOnlyDictionary<MemberInfo, ICqlColumnInfo<T>>(_columns.ToDictionary(column => column.MemberInfo));
            _columnsByMemberNG =
                new ReadOnlyDictionary<MemberInfo, ICqlColumnInfo>(_columns.ToDictionary(column => column.MemberInfo,
                                                                                         column =>
                                                                                             (ICqlColumnInfo)column));

            //Key subsets
            var partitionKeys = _columns.Where(column => column.IsPartitionKey).ToArray();
            _partitionKeys = new ReadOnlyCollection<ICqlColumnInfo<T>>(partitionKeys);
            _partitionKeysNG = new ReadOnlyCollection<ICqlColumnInfo>(partitionKeys);

            _partitionKeyTypes = partitionKeys.Select(info => info.CqlType).ToArray();

            var clusteringKeys = _columns.Where(column => column.IsClusteringKey).ToArray();
            _clusteringKeys = new ReadOnlyCollection<ICqlColumnInfo<T>>(clusteringKeys);
            _clusteringKeysNG = new ReadOnlyCollection<ICqlColumnInfo>(clusteringKeys);

            var normalColumns = _columns.Where(column => !column.IsClusteringKey && !column.IsPartitionKey).ToArray();
            _normalColumns = new ReadOnlyCollection<ICqlColumnInfo<T>>(normalColumns);
            _normalColumnsNG = new ReadOnlyCollection<ICqlColumnInfo>(normalColumns);
        }


        /// <summary>
        /// Gets a value indicating whether [is key space set].
        /// </summary>
        /// <value> <c>true</c> if [is key space set]; otherwise, <c>false</c> . </value>
        public bool IsKeySpaceSet { get; private set; }

        /// <summary>
        /// Gets the keyspace.
        /// </summary>
        /// <value> The keyspace. </value>
        public string Keyspace { get; private set; }

        /// <summary>
        /// Gets a value indicating whether [is table set].
        /// </summary>
        /// <value> <c>true</c> if [is table set]; otherwise, <c>false</c> . </value>
        public bool IsNameSet { get; private set; }

        /// <summary>
        /// Gets the table name.
        /// </summary>
        /// <value> The table. </value>
        public string Name { get; private set; }


        /// <summary>
        /// Gets the typeCode this accessor can handle
        /// </summary>
        /// <value> The typeCode. </value>
        public Type Type
        {
            get { return _type; }
        }

        /// <summary>
        /// Gets the partition keys.
        /// </summary>
        /// <value> The partition keys. </value>
        public ReadOnlyCollection<ICqlColumnInfo<T>> PartitionKeys
        {
            get { return _partitionKeys; }
        }

        /// <summary>
        /// Gets the clustering keys.
        /// </summary>
        /// <value> The clustering keys. </value>
        public ReadOnlyCollection<ICqlColumnInfo<T>> ClusteringKeys
        {
            get { return _clusteringKeys; }
        }

        /// <summary>
        /// Gets the normal (non-key) columns.
        /// </summary>
        /// <value> The normal columns. </value>
        public ReadOnlyCollection<ICqlColumnInfo<T>> NormalColumns
        {
            get { return _normalColumns; }
        }

        /// <summary>
        /// Gets all the columns.
        /// </summary>
        /// <value> The columns. </value>
        public ReadOnlyCollection<ICqlColumnInfo<T>> Columns
        {
            get { return _columns; }
        }

        /// <summary>
        /// Gets the columns by field or property member.
        /// </summary>
        /// <value> The columns by member. </value>
        public ReadOnlyDictionary<MemberInfo, ICqlColumnInfo<T>> ColumnsByMember
        {
            get { return _columnsByMember; }
        }

        /// <summary>
        /// Gets the columns by column name. When the Table or Keyspace is known the dictionary
        /// will contain entries where the column name is combined with the Table or Keyspace names.
        /// </summary>
        /// <value> The columns by member. </value>
        public ReadOnlyDictionary<string, ICqlColumnInfo<T>> ColumnsByName
        {
            get { return _columnsByName; }
        }

        /// <summary>
        /// Sets the entity properties.
        /// </summary>
        private void SetEntityProperties()
        {
            //set default keyspace and entity name to empty strings (nothing)
            Keyspace = null;
            Name = null;

            //set default name to class name if class is not anonymous
            _type = typeof(T);
            IsNameSet = !_type.IsAnonymous();
            if(IsNameSet)
                Name = _type.Name.ToLower();

            //check for CqlEntity attribute
            var entityAttribute = Attribute.GetCustomAttribute(_type, typeof(CqlEntityAttribute)) as CqlEntityAttribute;
            if(entityAttribute != null)
            {
                //overwrite keyspace if any
                IsKeySpaceSet = entityAttribute.Keyspace != null;
                if(IsKeySpaceSet)
                    Keyspace = entityAttribute.Keyspace;

                //set default name
                Name = entityAttribute.Name ?? Name;
            }
        }

        /// <summary>
        /// Create column info objects for all relevant fields or properties
        /// </summary>
        /// <returns></returns>
        private ICqlColumnInfo<T>[] GetColumns()
        {
            //create a column List
            var columns = new List<ICqlColumnInfo<T>>();

            //go over all properties
            foreach(PropertyInfo prop in _type.GetProperties())
            {
                if(ShouldIgnoreMember(prop))
                    continue;

                //create the column info object
                ICqlColumnInfo<T> info = CreateColumnInfo(prop, prop.PropertyType);

                columns.Add(info);
            }

            //go over all fields
            foreach(FieldInfo field in _type.GetFields())
            {
                if(ShouldIgnoreMember(field))
                    continue;

                //create the column info object
                ICqlColumnInfo<T> info = CreateColumnInfo(field, field.FieldType);

                columns.Add(info);
            }

            //sort the columns based on their order
            var sortedColumns = columns.OrderBy(col => col.Order.HasValue ? col.Order.Value : int.MaxValue);

            return sortedColumns.ToArray();
        }

        /// <summary>
        /// Checks wether the member must be ignored
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
        /// Creates the column information.
        /// </summary>
        /// <param name="prop">The property or field.</param>
        /// <param name="type">The type of the property or field.</param>
        /// <returns></returns>
        private static ICqlColumnInfo<T> CreateColumnInfo(MemberInfo prop, Type type)
        {
            var columnType = typeof(CqlColumnInfo<,>).MakeGenericType(typeof(T), type);
            var column = (ICqlColumnInfo<T>)Activator.CreateInstance(columnType, prop);
            return column;
        }


        /// <summary>
        /// Sets the key information.
        /// </summary>
        private static void SetKeyInfo(IEnumerable<ICqlColumnInfo<T>> columns)
        {
            bool isFirstKey = true;
            bool processingPartitionKeys = true;

            foreach(IKeyMember column in columns)
            {
                //check for column attribute
                var keyAttribute =
                    Attribute.GetCustomAttribute(column.MemberInfo, typeof(CqlKeyAttribute)) as CqlKeyAttribute;

                if(keyAttribute != null)
                {
                    column.IsPartitionKey = isFirstKey || keyAttribute.IsPartitionKey;
                    column.IsClusteringKey = !column.IsPartitionKey;

                    if(!processingPartitionKeys && column.IsPartitionKey)
                    {
                        throw new CqlException(
                            "Partition keys are not allowed after the first clustering keys. Make sure the column order is correct");
                    }

                    isFirstKey = false;
                    processingPartitionKeys = column.IsPartitionKey;
                }
                else
                {
                    column.IsPartitionKey = false;
                    column.IsClusteringKey = false;
                }
            }
        }

        /// <summary>
        /// Tries to get a value from the source, based on the column description
        /// </summary>
        /// <param name="columnName"> Name of the column. </param>
        /// <param name="source"> The source. </param>
        /// <param name="value"> The value. </param>
        /// <returns> true, if the value could be distilled from the source </returns>
        /// <exception cref="System.ArgumentNullException">columnName or source are null</exception>
        public bool TryGetValue<TValue>(string columnName, T source, out TValue value)
        {
            if(columnName == null)
                throw new ArgumentNullException("columnName");

            // ReSharper disable CompareNonConstrainedGenericWithNull
            if(source == null)
                // ReSharper restore CompareNonConstrainedGenericWithNull
                throw new ArgumentNullException("source");

            ICqlColumnInfo<T> column;
            if(_columnsByName.TryGetValue(columnName, out column))
            {
                value = column.Read<TValue>(source);
                return true;
            }

            value = default(TValue);
            return false;
        }

        /// <summary>
        /// Tries to set a property or field of the specified object, based on the column description
        /// </summary>
        /// <param name="columnName"> Name of the column. </param>
        /// <param name="target"> The target. </param>
        /// <param name="value"> The value. </param>
        /// <returns> true if the property or field value is set </returns>
        /// <exception cref="System.ArgumentNullException">columnName or target are null</exception>
        public bool TrySetValue<TValue>(string columnName, T target, TValue value)
        {
            if(columnName == null)
                throw new ArgumentNullException("columnName");

            // ReSharper disable CompareNonConstrainedGenericWithNull
            if(target == null)
                // ReSharper restore CompareNonConstrainedGenericWithNull
                throw new ArgumentNullException("target");


            ICqlColumnInfo<T> column;
            if(_columnsByName.TryGetValue(columnName, out column))
            {
                column.Write(target, value);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Sets the partition key based on the data found in a table entry.
        /// </summary>
        /// <param name="key"> The key. </param>
        /// <param name="value"> The value. </param>
        /// <exception cref="CqlException">Unable to read value for partition key column  + _partitionKeys[i].Name</exception>
        public void SetPartitionKey(PartitionKey key, T value)
        {
            int length = _partitionKeys.Count;
            if(length > 0)
            {
                var values = new object[length];
                for(int i = 0; i < length; i++)
                {
                    values[i] = _partitionKeys[i].Read<object>(value);
                }

                key.Set(_partitionKeyTypes, values);
            }
        }


        /// <summary>
        /// Gets the partition keys.
        /// </summary>
        /// <value>
        /// The partition keys.
        /// </value>
        ReadOnlyCollection<ICqlColumnInfo> IObjectAccessor.PartitionKeys
        {
            get { return _partitionKeysNG; }
        }

        /// <summary>
        /// Gets the clustering keys.
        /// </summary>
        /// <value>
        /// The clustering keys.
        /// </value>
        ReadOnlyCollection<ICqlColumnInfo> IObjectAccessor.ClusteringKeys
        {
            get { return _clusteringKeysNG; }
        }

        /// <summary>
        /// Gets the normal (non-key) columns.
        /// </summary>
        /// <value>
        /// The normal columns.
        /// </value>
        ReadOnlyCollection<ICqlColumnInfo> IObjectAccessor.NormalColumns
        {
            get { return _normalColumnsNG; }
        }

        /// <summary>
        /// Gets all the columns.
        /// </summary>
        /// <value>
        /// The columns.
        /// </value>
        ReadOnlyCollection<ICqlColumnInfo> IObjectAccessor.Columns
        {
            get { return _columnsNG; }
        }

        /// <summary>
        /// Gets the columns by field or property member.
        /// </summary>
        /// <value>
        /// The columns by member.
        /// </value>
        ReadOnlyDictionary<MemberInfo, ICqlColumnInfo> IObjectAccessor.ColumnsByMember
        {
            get { return _columnsByMemberNG; }
        }

        /// <summary>
        /// Gets the columns by column name. When the Table or Keyspace is known the dictionary
        /// will contain entries where the column name is combined with the Table or Keyspace names.
        /// </summary>
        /// <value>
        /// The columns by member.
        /// </value>
        ReadOnlyDictionary<string, ICqlColumnInfo> IObjectAccessor.ColumnsByName
        {
            get { return _columnsByNameNG; }
        }

        /// <summary>
        /// Tries to get a value from the source, based on the column description
        /// </summary>
        /// <param name="columnName">Name of the column.</param>
        /// <param name="source">The source.</param>
        /// <param name="value">The value.</param>
        /// <returns>
        /// true, if the value could be distilled from the source
        /// </returns>
        bool IObjectAccessor.TryGetValue(string columnName, object source, out object value)
        {
            return TryGetValue(columnName, (T)source, out value);
        }

        /// <summary>
        /// Tries to set a property or field of the specified object, based on the column description
        /// </summary>
        /// <param name="columnName">Name of the column.</param>
        /// <param name="target">The target.</param>
        /// <param name="value">The value.</param>
        /// <returns>
        /// true if the property or field value is set
        /// </returns>
        bool IObjectAccessor.TrySetValue(string columnName, object target, object value)
        {
            return TrySetValue(columnName, (T)target, value);
        }


        /// <summary>
        /// Sets the partition key based on the data found in a table entry.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        void IObjectAccessor.SetPartitionKey(PartitionKey key, object value)
        {
            SetPartitionKey(key, (T)value);
        }
    }
}