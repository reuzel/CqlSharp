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
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using CqlSharp.Annotations;
using CqlSharp.Protocol;

namespace CqlSharp.Serialization.Marshal
{
    /// <summary>
    /// The definition of a User Defined Type
    /// </summary>
    public class UserDefinedType<T> : CqlType<T>
    {
        private readonly string _keyspace;
        private readonly string _name;
        private readonly List<KeyValuePair<string, CqlType>> _fieldList;
        private readonly Dictionary<string, int> _fieldIndexes;

        /// <summary>
        /// Initializes a new instance of the <see cref="UserDefinedType{T}" /> class.
        /// </summary>
        /// <param name="keyspace">The keyspace.</param>
        /// <param name="name">The name.</param>
        /// <param name="fieldNames">The field names.</param>
        /// <param name="types">The field types.</param>
        public UserDefinedType([NotNull] string keyspace, [NotNull] string name,
                               [NotNull] IEnumerable<string> fieldNames,
                               [NotNull] IEnumerable<CqlType> types)
        {
            if(keyspace == null) throw new ArgumentNullException("keyspace");
            if(name == null) throw new ArgumentNullException("name");
            if(fieldNames == null) throw new ArgumentNullException("fieldNames");
            if(types == null) throw new ArgumentNullException("types");

            _keyspace = keyspace;
            _name = name;

            _fieldList = fieldNames.Zip(types, (n, t) => new KeyValuePair<string, CqlType>(n, t)).ToList();

            _fieldIndexes = new Dictionary<string, int>();
            int i = 0;
            foreach(var n in fieldNames)
                _fieldIndexes.Add(n, i++);
        }

        /// <summary>
        /// Gets the keyspace.
        /// </summary>
        /// <value>
        /// The keyspace.
        /// </value>
        public string Keyspace
        {
            get { return _keyspace; }
        }

        /// <summary>
        /// Gets the name.
        /// </summary>
        /// <value>
        /// The name.
        /// </value>
        public string Name
        {
            get { return _name; }
        }

        /// <summary>
        /// Gets the field names.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetFieldNames()
        {
            return _fieldList.Select(kvp => kvp.Key);
        }

        /// <summary>
        /// Gets the name of the field.
        /// </summary>
        /// <param name="i">The index of the field.</param>
        /// <returns></returns>
        public string GetFieldName(int i)
        {
            return _fieldList[i].Key;
        }

        /// <summary>
        /// Gets the type of the field.
        /// </summary>
        /// <param name="i">The index of the field.</param>
        /// <returns></returns>
        public CqlType GetFieldType(int i)
        {
            return _fieldList[i].Value;
        }

        /// <summary>
        /// Gets the index of the field with the given name.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>zero based index if the field was found, or -1 otherwise</returns>
        public int GetFieldIndex(string name)
        {
            int result;
            if(!_fieldIndexes.TryGetValue(name, out result))
                result = -1;

            return result;
        }

        /// <summary>
        /// Gets the number of fields in this User Defined Type
        /// </summary>
        /// <returns></returns>
        public int GetFieldCount()
        {
            return _fieldList.Count;
        }

        /// <summary>
        /// Gets the maximum size in bytes of values of this type.
        /// </summary>
        /// <value>
        /// The maximum size in bytes.
        /// </value>
        public override int Size
        {
            get { return 2000000000; }
        }

        /// <summary>
        /// Serializes the specified object.
        /// </summary>
        /// <typeparam name="TSource">The type of the source.</typeparam>
        /// <param name="source">The source object to serialize using this type.</param>
        /// <param name="protocolVersion"></param>
        /// <returns>
        /// byte array containing the serialized value of the source object
        /// </returns>
        /// <remarks>
        /// This method is overridden to prevent unnessecary creation and casting of UserDefined objects
        /// </remarks>
        public override byte[] Serialize<TSource>(TSource source, byte protocolVersion)
        {
            // ReSharper disable once CompareNonConstrainedGenericWithNull
            if(source == null)
                return null;

            var accessor = ObjectAccessor<TSource>.Instance;

            if(accessor.Columns.Count > _fieldList.Count)
            {
                throw new CqlException(string.Format("Type {0} is not compatible with CqlType {1}", typeof(TSource),
                                                     this));
            }

            //write all the components
            using(var stream = new MemoryStream())
            {
                for(int i = 0; i < accessor.Columns.Count; i++)
                {
                    var column = accessor.Columns[i];
                    if(column.CqlType != _fieldList[i].Value)
                    {
                        throw new CqlException(string.Format("Type {0} is not compatible with CqlType {1}",
                                                             typeof(TSource), this));
                    }

                    var rawValue = column.SerializeFrom(source, _fieldList[i].Value, protocolVersion);
                    stream.WriteByteArray(rawValue);
                }

                return stream.ToArray();
            }
        }

        public override byte[] Serialize(T value, byte protocolVersion)
        {
            return Serialize<T>(value, protocolVersion);
        }


        /// <summary>
        /// Deserializes the specified data to object of the given target type.
        /// </summary>
        /// <typeparam name="TTarget">The type of the target.</typeparam>
        /// <param name="data">The data to deserialize.</param>
        /// <param name="protocolVersion"></param>
        /// <returns>an object of the given type</returns>
        /// <remarks>
        /// This method is overridden to prevent unnessecary creation and casting of UserDefined objects
        /// </remarks>
        public override TTarget Deserialize<TTarget>(byte[] data, byte protocolVersion)
        {
            if(data == null)
                return default(TTarget);

            IObjectAccessor accessor;
            object result;

            if(typeof(TTarget) == typeof(object))
            {
                accessor = ObjectAccessor<T>.Instance;
                result = Activator.CreateInstance<T>();
            }
            else
            {
                accessor = ObjectAccessor<TTarget>.Instance;
                result = Activator.CreateInstance<TTarget>();
            }

            using(var stream = new MemoryStream(data))
            {
                foreach(var field in _fieldList)
                {
                    byte[] rawValue = stream.ReadByteArray();

                    ICqlColumnInfo column;
                    if(accessor.ColumnsByName.TryGetValue(field.Key, out column))
                        column.DeserializeTo(result, rawValue, field.Value, protocolVersion);
                }

                return (TTarget)result;
            }
        }

        /// <summary>
        /// Deserializes the specified data to object of the type corresponding to this CqlType.
        /// </summary>
        /// <param name="data">The data to deserialize.</param>
        /// <param name="protocolVersion"></param>
        /// <returns>a deserialized UserDefined object</returns>
        public override T Deserialize(byte[] data, byte protocolVersion)
        {
            return Deserialize<T>(data, protocolVersion);
        }

        /// <summary>
        /// Gets the CQL type code.
        /// </summary>
        /// <value>
        /// The CQL type code.
        /// </value>
        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Custom; }
        }

        /// <summary>
        /// Gets the full Cassandra name of the type (e.g. ListType(UTF8Type) ).
        /// </summary>
        /// <param name="builder"></param>
        /// <returns>
        /// The name of the type.
        /// </returns>
        public override void AppendTypeName(StringBuilder builder)
        {
            builder.Append("org.apache.cassandra.db.marshal.UserType(");
            builder.Append(_keyspace);
            builder.Append(',');
            builder.Append(_name.EncodeAsHex());
            foreach(var field in _fieldList)
            {
                builder.Append(',');
                builder.Append(field.Key.EncodeAsHex());
                builder.Append(':');
                field.Value.AppendTypeName(builder);
            }
            builder.Append(')');
        }

        public override DbType ToDbType()
        {
            return DbType.Object;
        }
    }
}