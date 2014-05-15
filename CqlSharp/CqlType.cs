using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Net;
using System.Numerics;

namespace CqlSharp
{
    /// <summary>
    /// Describes the typeCode of a single Cql Column or User Type field
    /// </summary>
    public class CqlType
    {


        #region native types

        public static readonly CqlType Ascii = new CqlType(CqlTypeCode.Ascii);
        public static readonly CqlType Bigint = new CqlType(CqlTypeCode.Bigint);
        public static readonly CqlType Blob = new CqlType(CqlTypeCode.Blob);
        public static readonly CqlType Boolean = new CqlType(CqlTypeCode.Boolean);
        public static readonly CqlType Counter = new CqlType(CqlTypeCode.Counter);
        public static readonly CqlType Decimal = new CqlType(CqlTypeCode.Decimal);
        public static readonly CqlType Double = new CqlType(CqlTypeCode.Double);
        public static readonly CqlType Float = new CqlType(CqlTypeCode.Float);
        public static readonly CqlType Inet = new CqlType(CqlTypeCode.Inet);
        public static readonly CqlType Int = new CqlType(CqlTypeCode.Int);
        public static readonly CqlType Text = new CqlType(CqlTypeCode.Text);
        public static readonly CqlType Timestamp = new CqlType(CqlTypeCode.Timestamp);
        public static readonly CqlType Timeuuid = new CqlType(CqlTypeCode.Timeuuid);
        public static readonly CqlType Uuid = new CqlType(CqlTypeCode.Uuid);
        public static readonly CqlType Varchar = new CqlType(CqlTypeCode.Varchar);
        public static readonly CqlType Varint = new CqlType(CqlTypeCode.Varint);

        #endregion


        private static readonly Dictionary<CqlTypeCode, Type> CqlType2Type = new Dictionary<CqlTypeCode, Type>
                                                                                 {
                                                                                     {
                                                                                         CqlTypeCode.Ascii, typeof (string)
                                                                                     },
                                                                                     {CqlTypeCode.Text, typeof (string)},
                                                                                     {
                                                                                         CqlTypeCode.Varchar,
                                                                                         typeof (string)
                                                                                     },
                                                                                     {CqlTypeCode.Blob, typeof (byte[])},
                                                                                     {
                                                                                         CqlTypeCode.Double,
                                                                                         typeof (double)
                                                                                     },
                                                                                     {CqlTypeCode.Float, typeof (float)},
                                                                                     {
                                                                                         CqlTypeCode.Decimal,
                                                                                         typeof (decimal)
                                                                                     },
                                                                                     {CqlTypeCode.Bigint, typeof (long)},
                                                                                     {
                                                                                         CqlTypeCode.Counter, typeof (long)
                                                                                     },
                                                                                     {CqlTypeCode.Int, typeof (int)},
                                                                                     {
                                                                                         CqlTypeCode.Boolean, typeof (bool)
                                                                                     },
                                                                                     {CqlTypeCode.Uuid, typeof (Guid)},
                                                                                     {
                                                                                         CqlTypeCode.Timeuuid,
                                                                                         typeof (Guid)
                                                                                     },
                                                                                     {
                                                                                         CqlTypeCode.Inet,
                                                                                         typeof (IPAddress)
                                                                                     },
                                                                                     {
                                                                                         CqlTypeCode.Varint,
                                                                                         typeof (BigInteger)
                                                                                     },
                                                                                     {
                                                                                         CqlTypeCode.Timestamp,
                                                                                         typeof (DateTime)
                                                                                     }
                                                                                 };

        private static readonly Dictionary<Type, CqlType> Type2CqlType = new Dictionary<Type, CqlType>
                                                                             {
                                                                                 {
                                                                                     typeof (string),
                                                                                     Varchar
                                                                                 },
                                                                                 {
                                                                                     typeof (byte[]),
                                                                                     Blob
                                                                                 },
                                                                                 {
                                                                                     typeof (double),
                                                                                     Double
                                                                                 },
                                                                                 {
                                                                                     typeof (float),
                                                                                     Float
                                                                                 },
                                                                                 {
                                                                                     typeof (decimal),
                                                                                     Decimal
                                                                                 },
                                                                                 {
                                                                                     typeof (long),
                                                                                     Bigint
                                                                                 },
                                                                                 {
                                                                                     typeof (int),
                                                                                     Int
                                                                                 },
                                                                                 {
                                                                                     typeof (bool),
                                                                                     Boolean
                                                                                 },
                                                                                 {
                                                                                     typeof (Guid),
                                                                                     Uuid
                                                                                 },
                                                                                 {
                                                                                     typeof (IPAddress),
                                                                                     Inet
                                                                                 },
                                                                                 {
                                                                                     typeof (BigInteger),
                                                                                     Varint
                                                                                 },
                                                                                 {
                                                                                     typeof (DateTime),
                                                                                     Timestamp
                                                                                 }
                                                                             };

        private static readonly Dictionary<CqlTypeCode, DbType> CqlType2DbType = new Dictionary<CqlTypeCode, DbType>
                                                                                     {
                                                                                         {
                                                                                             CqlTypeCode.Ascii,
                                                                                             DbType.AnsiString
                                                                                         },
                                                                                         {
                                                                                             CqlTypeCode.Text,
                                                                                             DbType.String
                                                                                         },
                                                                                         {
                                                                                             CqlTypeCode.Varchar,
                                                                                             DbType.String
                                                                                         },
                                                                                         {
                                                                                             CqlTypeCode.Blob,
                                                                                             DbType.Binary
                                                                                         },
                                                                                         {
                                                                                             CqlTypeCode.Double,
                                                                                             DbType.Double
                                                                                         },
                                                                                         {
                                                                                             CqlTypeCode.Float,
                                                                                             DbType.Single
                                                                                         },
                                                                                         {
                                                                                             CqlTypeCode.Decimal,
                                                                                             DbType.Decimal
                                                                                         },
                                                                                         {
                                                                                             CqlTypeCode.Bigint,
                                                                                             DbType.Int64
                                                                                         },
                                                                                         {
                                                                                             CqlTypeCode.Counter,
                                                                                             DbType.Int64
                                                                                         },
                                                                                         {CqlTypeCode.Int, DbType.Int32},
                                                                                         {
                                                                                             CqlTypeCode.Boolean,
                                                                                             DbType.Boolean
                                                                                         },
                                                                                         {CqlTypeCode.Uuid, DbType.Guid},
                                                                                         {
                                                                                             CqlTypeCode.Timeuuid,
                                                                                             DbType.Guid
                                                                                         },
                                                                                         {
                                                                                             CqlTypeCode.Varint,
                                                                                             DbType.VarNumeric
                                                                                         },
                                                                                         {
                                                                                             CqlTypeCode.Timestamp,
                                                                                             DbType.DateTime
                                                                                         }
                                                                                     };

        private static readonly Dictionary<DbType, CqlType> DbType2CqlType = new Dictionary<DbType, CqlType>
                                                                                     {
                                                                                         {
                                                                                             DbType.AnsiString,
                                                                                             Ascii
                                                                                         },
                                                                                         {
                                                                                             DbType.Int64,
                                                                                             Bigint
                                                                                         },
                                                                                         {DbType.Guid, Uuid},
                                                                                         {
                                                                                             DbType.Binary,
                                                                                             Blob
                                                                                         },
                                                                                         {
                                                                                             DbType.DateTime,
                                                                                             Timestamp
                                                                                         },
                                                                                         {
                                                                                             DbType.Single,
                                                                                             Float
                                                                                         },
                                                                                         {
                                                                                             DbType.Double,
                                                                                             Double
                                                                                         },
                                                                                         {
                                                                                             DbType.Decimal,
                                                                                             Decimal
                                                                                         },
                                                                                         {DbType.Int32, Int},
                                                                                         {
                                                                                             DbType.Boolean,
                                                                                             Boolean
                                                                                         },
                                                                                         {
                                                                                             DbType.VarNumeric,
                                                                                             Varint
                                                                                         },
                                                                                         {
                                                                                             DbType.String,
                                                                                             Varchar
                                                                                         },
                                                                                     };


        /// <summary>
        /// Initializes a new instance of the <see cref="CqlType"/> class.
        /// </summary>
        /// <param name="typeCode">The typeCode.</param>
        /// <exception cref="System.ArgumentException">
        /// Please provide key and value typeCodes for a map typeCode;typeCode
        /// or
        /// Please provide value typeCode for a set or list typeCode;typeCode
        /// or
        /// Please provide custom data typeCode for a set or list typeCode;typeCode
        /// </exception>
        public CqlType(CqlTypeCode typeCode)
        {
            if (typeCode == CqlTypeCode.Map)
                throw new ArgumentException("Please provide key and value type for a map typeCode", "typeCode");

            if (typeCode == CqlTypeCode.Set || typeCode == CqlTypeCode.List)
                throw new ArgumentException("Please provide value typeCode for a set or list typeCode", "typeCode");

            if (typeCode == CqlTypeCode.Custom)
                throw new ArgumentException("Please provide custom data typeCode for a set or list typeCode", "typeCode");

            CqlTypeCode = typeCode;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlType"/> class.
        /// </summary>
        /// <param name="typeCode">The typeCode.</param>
        /// <param name="customType">Type of the custom.</param>
        /// <exception cref="System.ArgumentNullException">customType</exception>
        /// <exception cref="System.ArgumentException">Type must be set to CqlTypeCode.Custom when a CustomType string is provided;typeCode</exception>
        public CqlType(CqlTypeCode typeCode, string customType)
        {
            if (customType == null)
                throw new ArgumentNullException("customType");

            if (typeCode != CqlTypeCode.Custom)
                throw new ArgumentException(
                    "Type must be set to CqlTypeCode.Custom when a CustomType string is provided", "typeCode");

            CqlTypeCode = typeCode;
            CustomType = customType;

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlType"/> class.
        /// </summary>
        /// <param name="typeCode">The typeCode.</param>
        /// <param name="collectionValueTypeCode">Type of the collection value.</param>
        /// <exception cref="System.ArgumentException">Type must be CqlTypeCode.Set or CqlTypeCode.List when only the CollectionValueType is provided;typeCode</exception>
        public CqlType(CqlTypeCode typeCode, CqlType collectionValueTypeCode)
        {
            if (typeCode != CqlTypeCode.Set && typeCode != CqlTypeCode.List)
                throw new ArgumentException(
                    "Type must be CqlTypeCode.Set or CqlTypeCode.List when only the CollectionValueType is provided",
                    "typeCode");

            if (collectionValueTypeCode.CqlTypeCode == CqlTypeCode.List ||
                collectionValueTypeCode.CqlTypeCode == CqlTypeCode.Set ||
                collectionValueTypeCode.CqlTypeCode == CqlTypeCode.Map)
                throw new ArgumentException("Collection value can not be another list, set or map");

            CqlTypeCode = typeCode;
            CollectionValueType = collectionValueTypeCode;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlType"/> class.
        /// </summary>
        /// <param name="typeCode">The typeCode.</param>
        /// <param name="collectionKeyType">Type of the collection key.</param>
        /// <param name="collectionValueType">Type of the collection value.</param>
        /// <exception cref="System.ArgumentException">Type must be CqlTypeCode.Map when CollectionKeyType and CollectionValueType are provided;typeCode</exception>
        public CqlType(CqlTypeCode typeCode, CqlType collectionKeyType, CqlType collectionValueType)
        {
            if (typeCode != CqlTypeCode.Map)
                throw new ArgumentException(
                    "Type must be CqlTypeCode.Map when CollectionKeyType and CollectionValueType are provided",
                    "typeCode");

            if (collectionKeyType.CqlTypeCode == CqlTypeCode.List || collectionKeyType.CqlTypeCode == CqlTypeCode.Set ||
                collectionKeyType.CqlTypeCode == CqlTypeCode.Map)
                throw new ArgumentException("Map key type can not be another list, set or map", "collectionKeyType");

            if (collectionValueType.CqlTypeCode == CqlTypeCode.List ||
                collectionValueType.CqlTypeCode == CqlTypeCode.Set || collectionValueType.CqlTypeCode == CqlTypeCode.Map)
                throw new ArgumentException("Collection value can not be another list, set or map",
                                            "collectionValueType");

            CqlTypeCode = typeCode;
            CollectionKeyType = collectionKeyType;
            CollectionValueType = collectionValueType;
        }

        /// <summary>
        /// Gets the typeCode of the CQL.
        /// </summary>
        /// <value>
        /// The typeCode of the CQL.
        /// </value>
        public CqlTypeCode CqlTypeCode { get; private set; }

        /// <summary>
        /// Gets the custom typeCode name.
        /// </summary>
        /// <value>
        /// The typeCode of the custom.
        /// </value>
        public string CustomType { get; private set; }

        /// <summary>
        /// Gets the typeCode of the collection key.
        /// </summary>
        /// <value>
        /// The typeCode of the collection key.
        /// </value>
        public CqlType CollectionKeyType { get; private set; }

        /// <summary>
        /// Gets the typeCode of the collection value.
        /// </summary>
        /// <value>
        /// The typeCode of the collection value.
        /// </value>
        public CqlType CollectionValueType { get; private set; }

        /// <summary>
        ///   Returns the .NET typeCode representing the column typeCode
        /// </summary>
        /// <returns> </returns>
        public Type ToType()
        {
            Type type;
            switch (CqlTypeCode)
            {
                case CqlTypeCode.Map:
                    Type genericMapType = typeof(Dictionary<,>);

                    Debug.Assert(CollectionKeyType != null, "a map should have a Key typeCode");
                    Debug.Assert(CollectionValueType != null, "a map should have a Value typeCode");

                    type = genericMapType.MakeGenericType(CollectionKeyType.ToType(),
                                                          CollectionValueType.ToType());
                    break;

                case CqlTypeCode.Set:
                    Type genericSetType = typeof(HashSet<>);

                    Debug.Assert(CollectionValueType != null, "a set should have a Value typeCode");

                    type = genericSetType.MakeGenericType(CollectionValueType.ToType());
                    break;

                case CqlTypeCode.List:
                    Type genericListType = typeof(List<>);
                    Debug.Assert(CollectionValueType != null, "a list should have a Value typeCode");

                    type = genericListType.MakeGenericType(CollectionValueType.ToType());
                    break;

                //TODO: custom support here

                default:
                    if (!CqlType2Type.TryGetValue(CqlTypeCode, out type))
                        throw new ArgumentException("Unsupported typeCode");
                    break;
            }

            return type;
        }

        /// <summary>
        ///   Constructs a CqlType based on the provided Type
        /// </summary>
        /// <param name="type"> The type </param>
        public static CqlType FromType(Type type)
        {
            CqlType cqlType;
            if (!Type2CqlType.TryGetValue(type, out cqlType))
            {
                if (type.IsGenericType)
                {
                    var genericType = type.GetGenericTypeDefinition();

                    //check for nullable types
                    if (genericType == typeof(Nullable<>))
                    {
                        return FromType(type.GetGenericArguments()[0]);
                    }

                    if (genericType == typeof(List<>))
                    {
                        Type listType = type.GetGenericArguments()[0];
                        CqlType valueType = FromType(listType);
                        return new CqlType(CqlTypeCode.List, valueType);
                    }

                    if (genericType == typeof(HashSet<>))
                    {
                        Type setType = type.GetGenericArguments()[0];
                        CqlType valueType = FromType(setType);
                        return new CqlType(CqlTypeCode.Set, valueType);
                    }

                    if (genericType == typeof(Dictionary<,>))
                    {
                        var keyType = FromType(type.GetGenericArguments()[0]);
                        var valueType = FromType(type.GetGenericArguments()[1]);
                        return new CqlType(CqlTypeCode.Map, keyType, valueType);
                    }

                    //TODO custom support here

                    throw new CqlException("Unsupported type");
                }
            }

            return cqlType;
        }


        /// <summary>
        ///   gets the corresponding the DbType
        /// </summary>
        /// <returns> </returns>
        public DbType ToDbType()
        {
            DbType type;

            if (CqlType2DbType.TryGetValue(CqlTypeCode, out type))
            {
                return type;
            }

            return DbType.Object;
        }

        /// <summary>
        /// Determines whether the specified typeCode is a supported CQL typeCode
        /// </summary>
        /// <param name="type">The typeCode.</param>
        /// <returns></returns>
        /// <exception cref="System.NotSupportedException">CqlTypeCode can not be mapped to a valid CQL typeCode</exception>
        public static bool IsSupportedCqlType(Type type)
        {
            try
            {
                //TODO refactor such that no exceptions is thrown
                return FromType(type) != null;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Returns a <see cref="System.String" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            switch (CqlTypeCode)
            {
                case CqlTypeCode.List:
                    return string.Format("list<{0}>", CollectionValueType);
                case CqlTypeCode.Set:
                    return string.Format("set<{0}>", CollectionValueType);
                case CqlTypeCode.Map:
                    return string.Format("map<{0},{1}>", CollectionKeyType, CollectionValueType);
                case CqlTypeCode.Custom:
                    return CustomType;
                default:
                    return CqlTypeCode.ToString();
            }
        }

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
    }
}
