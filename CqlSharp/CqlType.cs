using CqlSharp.Serialization;
using CqlSharp.Serialization.Marshal;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net;
using System.Numerics;

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
                                        { DbType.AnsiString, Ascii},
                                        { DbType.Int64, Bigint },
                                        { DbType.Guid, Uuid },
                                        { DbType.Binary, Blob },
                                        { DbType.DateTime, Timestamp },
                                        { DbType.Single, Float },
                                        { DbType.Double, Double },
                                        { DbType.Decimal, Decimal },
                                        { DbType.Int32, Int },
                                        { DbType.Boolean, Boolean },
                                        { DbType.VarNumeric, Varint },
                                        { DbType.String, Varchar },
                                    };

        }

        public static CqlType CreateType(CqlTypeCode tc, params object[] arguments)
        {
            return TypeCodeMap[(short)tc].CreateType(arguments);
        }

        public static CqlType CreateType(string typeName)
        {
            return TypeName2CqlType.GetOrAdd(typeName, (name) =>
            {
                var tp = new TypeParser(name);
                return tp.CreateType();
            });
        }

        public static CqlType CreateType(Type type)
        {
            return Type2CqlType.GetOrAdd(type, (newType) =>
            {
                if (newType.IsGenericType)
                {
                    var genericType = type.GetGenericTypeDefinition();

                    //check for nullable types
                    if (genericType == typeof(Nullable<>))
                    {
                        return CreateType(newType.GetGenericArguments()[0]);
                    }

                    var interfaces =
                        newType.GetInterfaces()
                        .Where(i => i.IsGenericType)
                        .Select(i => i.GetGenericTypeDefinition());


                    //check for collection types
                    if (interfaces.Any(i => i == typeof(IDictionary<,>)))
                        return new MapTypeFactory().CreateType(newType);

                    if (interfaces.Any(i => i == typeof(ISet<>)))
                        return new SetTypeFactory().CreateType(newType);

                    if (interfaces.Any(i => i == typeof(IList<>)))
                        return new ListTypeFactory().CreateType(newType);
                }

                //TODO custom support here

                throw new Exception("Unsupported type");

            });
        }

        public abstract CqlTypeCode CqlTypeCode { get; }
        public abstract string TypeName { get; }
        public abstract Type Type { get; }
        public abstract byte[] Serialize<TSource>(TSource source);
        public abstract TTarget Deserialize<TTarget>(byte[] data);


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
            {
                return cqlType;
            }

            throw new ArgumentOutOfRangeException("type", type, "CqlType can not be derived from the given DbType");
        }

        /// <summary>
        ///   gets the corresponding the DbType
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
        public bool Equals(CqlType other)
        {
            if (CqlTypeCode != other.CqlTypeCode)
                return false;

            if (GetType() != other.GetType())
                return false;

            if (CqlTypeCode == CqlTypeCode.Custom)
                return other.TypeName.Equals(TypeName, StringComparison.OrdinalIgnoreCase);

            return false;
        }

        /// <summary>
        /// Determines whether the specified <see cref="System.Object" }, is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="System.Object" /> to compare with this instance.</param>
        /// <returns>
        ///   <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
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

        public static bool operator ==(CqlType left, CqlType right)
        {
            return Equals(left, right);
        }

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

            return CqlTypeCode.ToString();
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

        public override byte[] Serialize<TSource>(TSource source)
        {
            T value = Converter.ChangeType<TSource, T>(source);
            return Serialize(value);
        }

        public override TTarget Deserialize<TTarget>(byte[] data)
        {
            T value = Deserialize(data);
            return Converter.ChangeType<T, TTarget>(value);
        }

        public abstract byte[] Serialize(T value);
        public abstract T Deserialize(byte[] data);
    }
}
