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
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Numerics;
using System.Text;
using CqlSharp.Serialization;
using CqlSharp.Serialization.Marshal;

namespace CqlSharp
{
    /// <summary>
    /// Represents a CqlType. Do not instantiate CqlTypes directly. Instead use one of the CreateType method overloads.
    /// </summary>
    public abstract class CqlType : IEquatable<CqlType>
    {
        #region Native types

        public static readonly CqlType Ascii = AsciiType.Instance;
        public static readonly CqlType Bigint = LongType.Instance;
        public static readonly CqlType Blob = BytesType.Instance;
        public static readonly CqlType Boolean = BooleanType.Instance;
        public static readonly CqlType Counter = CounterColumnType.Instance;
        public static readonly CqlType Decimal = DecimalType.Instance;
        public static readonly CqlType Double = DoubleType.Instance;
        public static readonly CqlType Float = FloatType.Instance;
        public static readonly CqlType Inet = InetAddressType.Instance;
        public static readonly CqlType Int = Int32Type.Instance;
        public static readonly CqlType Text = UTF8Type.Instance;
        public static readonly CqlType Timestamp = TimestampType.Instance;
        public static readonly CqlType Uuid = UUIDType.Instance;
        public static readonly CqlType Varchar = UTF8Type.Instance;
        public static readonly CqlType Varint = IntegerType.Instance;
        public static readonly CqlType TimeUuid = TimeUUIDType.Instance;

        #endregion

        private static readonly ITypeFactory[] TypeCodeMap;
        private static readonly ConcurrentDictionary<Type, CqlType> Type2CqlType;
        private static readonly ConcurrentDictionary<String, CqlType> TypeName2CqlType;
        private static readonly Dictionary<DbType, CqlType> DbType2CqlType;

        /// <summary>
        /// The type serializers. Cached version of the serializers, giving efficient access to the generic serialize methods from
        /// objects
        /// </summary>
        private static readonly ConcurrentDictionary<Type, Func<CqlType, object, byte, byte[]>> TypeSerializers;

        /// <summary>
        /// The cached version of the type name
        /// </summary>
        private string _typeName;

        static CqlType()
        {
            //populate typeCode to CqlType map
            TypeCodeMap = new ITypeFactory[Enum.GetValues(typeof(CqlTypeCode)).Cast<int>().Max() + 1];
            TypeCodeMap[(short)CqlTypeCode.Ascii] = new AsciiTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Bigint] = new LongTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Blob] = new BytesTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Boolean] = new BooleanTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Counter] = new CounterColumnTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Decimal] = new DecimalTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Double] = new DoubleTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Float] = new FloatTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Inet] = new InetAddressTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Int] = new Int32TypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Text] = new UTF8TypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Timestamp] = new TimestampTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Uuid] = new UUIDTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Varchar] = new UTF8TypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Varint] = new IntegerTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Timeuuid] = new TimeUUIDTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.List] = new ListTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Set] = new SetTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Map] = new MapTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.UserDefinedType] = new UserDefinedTypeFactory();
            TypeCodeMap[(short)CqlTypeCode.Tuple] = new TupleTypeFactory();

            //populate .net type to CqlType map with all native classes
            Type2CqlType = new ConcurrentDictionary<Type, CqlType>();
            Type2CqlType[typeof(string)] = UTF8Type.Instance;
            Type2CqlType[typeof(long)] = LongType.Instance;
            Type2CqlType[typeof(byte[])] = BytesType.Instance;
            Type2CqlType[typeof(bool)] = BooleanType.Instance;
            Type2CqlType[typeof(decimal)] = DecimalType.Instance;
            Type2CqlType[typeof(double)] = DoubleType.Instance;
            Type2CqlType[typeof(float)] = FloatType.Instance;
            Type2CqlType[typeof(IPAddress)] = InetAddressType.Instance;
            Type2CqlType[typeof(int)] = Int32Type.Instance;
            Type2CqlType[typeof(DateTime)] = TimestampType.Instance;
            Type2CqlType[typeof(Guid)] = UUIDType.Instance;
            Type2CqlType[typeof(BigInteger)] = IntegerType.Instance;

            TypeName2CqlType = new ConcurrentDictionary<string, CqlType>();

            DbType2CqlType = new Dictionary<DbType, CqlType>
            {
                {DbType.AnsiString, Ascii},
                {DbType.Int64, Bigint},
                {DbType.Guid, Uuid},
                {DbType.Binary, Blob},
                {DbType.DateTime, Timestamp},
                {DbType.Single, Float},
                {DbType.Double, Double},
                {DbType.Decimal, Decimal},
                {DbType.Int32, Int},
                {DbType.Boolean, Boolean},
                {DbType.VarNumeric, Varint},
                {DbType.String, Varchar},
            };

            TypeSerializers = new ConcurrentDictionary<Type, Func<CqlType, object, byte, byte[]>>();
        }

        /// <summary>
        /// Creates a CqlType from the given typecode and a set of construction arguments
        /// </summary>
        /// <param name="tc">The CqlTypeCode</param>
        /// <param name="arguments">The arguments. This are different per type</param>
        /// <returns>A CqlType, constructed from the CqlTypeCode and arguments</returns>
        public static CqlType CreateType(CqlTypeCode tc, params object[] arguments)
        {
            return TypeCodeMap[(short)tc].CreateType(arguments);
        }

        /// <summary>
        /// Creates a CqlType from the given full Cassandra type name
        /// </summary>
        /// <param name="typeName">Name of the type. (e.g. ListType(UTF8Type) )</param>
        /// <returns>a CqlType representing the full type name</returns>
        public static CqlType CreateType(string typeName)
        {
            return TypeName2CqlType.GetOrAdd(typeName, name =>
            {
                var tp = new TypeParser(name);
                return tp.ReadCqlType();
            });
        }

        /// <summary>
        /// Creates a CqlType by reflecting the given .NET type
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>A CqlType that can be used to serialize/deserialize the given .NET type</returns>
        public static CqlType CreateType(Type type)
        {
            return Type2CqlType.GetOrAdd(type, newType =>
            {
                if (newType.IsGenericType)
                {
                    var genericType = type.GetGenericTypeDefinition();

                    //check for nullable types
                    if (genericType == typeof(Nullable<>))
                        return CreateType(newType.GetGenericArguments()[0]);

                    //check for tupleTypes
                    if (TypeExtensions.TupleTypes.Contains(genericType))
                        return new TupleTypeFactory().CreateType(newType);
                }

               
                //get all generic interfaces the type implements
                var interfaces = newType.GetInterfaces()
                           .Where(i => i.IsGenericType)
                           .Select(i => i.GetGenericTypeDefinition())
                           .ToList();

                //add the type self if it's a generic interface itself
                if(newType.IsInterface && newType.IsGenericType)
                    interfaces.Add(newType.GetGenericTypeDefinition());
    
                //check for map types
                if (interfaces.Any(i => i == typeof(IDictionary<,>)))
                    return new MapTypeFactory().CreateType(newType);

                //check for set types
                if (interfaces.Any(i => i == typeof(ISet<>)))
                    return new SetTypeFactory().CreateType(newType);

                //check for list types
                if (interfaces.Any(i => i == typeof(IList<>) || i == typeof(IEnumerable<>)))
                    return new ListTypeFactory().CreateType(newType);

                //check for user types
                var userTypeAttribute =
                    Attribute.GetCustomAttribute(newType, typeof(CqlUserTypeAttribute)) as CqlUserTypeAttribute;
                if (userTypeAttribute != null)
                    return new UserDefinedTypeFactory().CreateType(newType);

                //check for custom types
                var customAttribute =
                    Attribute.GetCustomAttribute(newType, typeof(CqlCustomTypeAttribute)) as CqlCustomTypeAttribute;
                if (customAttribute != null)
                    return customAttribute.CreateFactory().CreateType(newType);

                if (newType.IsAnonymous())
                    return new AnonymousTypeFactory().CreateType(newType);

                throw new CqlException(string.Format("Unable to map type {0} to a CqlType", newType.ToString()));
            });
        }

        /// <summary>
        /// Gets the CQL type code.
        /// </summary>
        /// <value>
        /// The CQL type code.
        /// </value>
        public abstract CqlTypeCode CqlTypeCode { get; }

        /// <summary>
        /// Gets the full Cassandra name of the type (e.g. ListType(UTF8Type) ).
        /// </summary>
        /// <returns>
        /// The name of the type.
        /// </returns>
        public string TypeName
        {
            get
            {
                if (_typeName == null)
                {
                    var builder = new StringBuilder();
                    AppendTypeName(builder);
                    _typeName = builder.ToString();
                }

                return _typeName;
            }
        }

        /// <summary>
        /// Adds the name of the type to the provided builder.
        /// </summary>
        /// <param name="builder">The builder.</param>
        public abstract void AppendTypeName(StringBuilder builder);

        /// <summary>
        /// Gets the .NET type
        /// </summary>
        /// <value>
        /// The type.
        /// </value>
        public abstract Type Type { get; }

        /// <summary>
        /// Gets the maximum size in bytes of values of this type.
        /// </summary>
        /// <value>
        /// The maximum size in bytes.
        /// </value>
        public abstract int Size { get; }

        /// <summary>
        /// Serializes the specified object.
        /// </summary>
        /// <param name="source">The source object to serialize using this type.</param>
        /// <param name="protocolVersion"></param>
        /// <returns>byte array containing the serialized value of the source object</returns>
        /// <remarks>This method may try to convert the source to a type serializable by this type</remarks>
        public byte[] Serialize(object source, byte protocolVersion)
        {
            var serializer = TypeSerializers.GetOrAdd(source.GetType(), type =>
            {
                var parameter = Expression.Parameter(typeof(object));
                var version = Expression.Parameter(typeof(byte));
                var instance = Expression.Parameter(typeof(CqlType));
                var call = Expression.Call(instance, "Serialize", new[] { type }, Expression.Convert(parameter, type),
                                           version);
                var lambda = Expression.Lambda<Func<CqlType, object, byte, byte[]>>(call,
                                                                                    string.Format(
                                                                                        "CqlType.Serialize<{0}>",
                                                                                        type.Name),
                                                                                    new[] { instance, parameter, version });
                return lambda.Compile();
            });

            return serializer(this, source, protocolVersion);
        }

        /// <summary>
        /// Serializes the specified object.
        /// </summary>
        /// <typeparam name="TSource">The type of the source.</typeparam>
        /// <param name="source">The source object to serialize using this type.</param>
        /// <param name="protocolVersion">protocol version of the underlying connection</param>
        /// <returns>
        /// byte array containing the serialized value of the source object
        /// </returns>
        /// <remarks>
        /// This method may try to convert the source to a type serializable by this type
        /// </remarks>
        public abstract byte[] Serialize<TSource>(TSource source, byte protocolVersion);

        /// <summary>
        /// Deserializes the specified data to object of the given target type.
        /// </summary>
        /// <typeparam name="TTarget">The type of the target.</typeparam>
        /// <param name="data">The data to deserialize.</param>
        /// <param name="protocolVersion">protocol version of the underlying connection</param>
        /// <returns>an object of the given type</returns>
        /// <remarks>The result may be type converted version of the actual deserialized value</remarks>
        public abstract TTarget Deserialize<TTarget>(byte[] data, byte protocolVersion);


        /// <summary>
        /// Creates a CqlType from the type of the database.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentOutOfRangeException">type;CqlType can not be derived from the given DbType</exception>
        internal static CqlType FromDbType(DbType type)
        {
            CqlType cqlType;

            if (DbType2CqlType.TryGetValue(type, out cqlType))
                return cqlType;

            throw new ArgumentOutOfRangeException("type", type, "CqlType can not be derived from the given DbType");
        }

        /// <summary>
        /// gets the corresponding the DbType
        /// </summary>
        /// <returns> </returns>
        public abstract DbType ToDbType();

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <param name="other">An object to compare with this object.</param>
        /// <returns>
        /// true if the current object is equal to the <paramref name="other" /> parameter; otherwise, false.
        /// </returns>
        public virtual bool Equals(CqlType other)
        {
            if (CqlTypeCode != other.CqlTypeCode)
                return false;

            if (CqlTypeCode == CqlTypeCode.Custom)
                return other.TypeName.Equals(TypeName, StringComparison.OrdinalIgnoreCase);

            return true;
        }

        /// <summary>
        /// Determines whether the specified <see cref="System.Object" />, is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="System.Object" /> to compare with this instance.</param>
        /// <returns>
        /// <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (!(obj is CqlType)) return false;
            return Equals((CqlType)obj);
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table.
        /// </returns>
        public override int GetHashCode()
        {
            return TypeName.GetHashCode();
        }

        /// <summary>
        /// Implements the operator ==.
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator ==(CqlType left, CqlType right)
        {
            return Equals(left, right);
        }

        /// <summary>
        /// Implements the operator !=.
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator !=(CqlType left, CqlType right)
        {
            return !Equals(left, right);
        }

        /// <summary>
        /// Returns a <see cref="System.String" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            if (CqlTypeCode == CqlTypeCode.Custom)
                return TypeName;

            return CqlTypeCode.ToString().ToLower();
        }
    }

    /// <summary>
    /// Typed version of the CqlType class. Implementers of new types should subclass this class
    /// </summary>
    /// <typeparam name="T">the .NET type represented by the given type</typeparam>
    public abstract class CqlType<T> : CqlType
    {
        public override Type Type
        {
            get { return typeof(T); }
        }

        /// <summary>
        /// Serializes the specified object.
        /// </summary>
        /// <typeparam name="TSource">The type of the source.</typeparam>
        /// <param name="source">The source object to serialize using this type.</param>
        /// <param name="protocolVersion">protocol version of the underlying connection</param>
        /// <returns>
        /// byte array containing the serialized value of the source object
        /// </returns>
        /// <remarks>
        /// This method may try to convert the source to a type serializable by this type
        /// </remarks>
        public override byte[] Serialize<TSource>(TSource source, byte protocolVersion)
        {
            // ReSharper disable once CompareNonConstrainedGenericWithNull
            if (source == null)
                return null;

            T value = Converter.ChangeType<TSource, T>(source);
            return Serialize(value, protocolVersion);
        }

        /// <summary>
        /// Deserializes the specified data to object of the given target type.
        /// </summary>
        /// <typeparam name="TTarget">The type of the target.</typeparam>
        /// <param name="data">The data to deserialize.</param>
        /// <param name="protocolVersion">protocol version of the underlying connection</param>
        /// <returns>an object of the given type</returns>
        /// <remarks>The result may be type converted version of the actual deserialized value</remarks>
        public override TTarget Deserialize<TTarget>(byte[] data, byte protocolVersion)
        {
            if (data == null)
                return default(TTarget);

            T value = Deserialize(data, protocolVersion);
            return Converter.ChangeType<T, TTarget>(value);
        }

        /// <summary>
        /// Serializes the specified object.
        /// </summary>
        /// <param name="value">The value to serialize using this type.</param>
        /// <param name="protocolVersion">protocol version of the underlying connection</param>
        /// <returns>
        /// byte array containing the serialized version of the value
        /// </returns>
        public abstract byte[] Serialize(T value, byte protocolVersion);

        /// <summary>
        /// Deserializes the specified data to object of the type corresponding to this CqlType.
        /// </summary>
        /// <param name="data">The data to deserialize.</param>
        /// <param name="protocolVersion">protocol version of the underlying connection</param>
        /// <returns>an object of the T</returns>
        public abstract T Deserialize(byte[] data, byte protocolVersion);
    }
}