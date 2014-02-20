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

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Net;
using System.Numerics;

namespace CqlSharp
{
    public enum CqlType
    {
        Custom = 0x0000,

        Ascii = 0x0001,

        Bigint = 0x0002,

        Blob = 0x0003,

        Boolean = 0x0004,

        Counter = 0x0005,

        Decimal = 0x0006,

        Double = 0x0007,

        Float = 0x0008,

        Int = 0x0009,

        Text = 0x000A,

        Timestamp = 0x000B,

        Uuid = 0x000C,

        Varchar = 0x000D,

        Varint = 0x000E,

        Timeuuid = 0x000F,

        Inet = 0x0010,

        List = 0x0020,

        Map = 0x0021,

        Set = 0x0022
    }

    public static class CqlTypeExtensions
    {
        private static readonly Dictionary<CqlType, Type> CqlType2Type = new Dictionary<CqlType, Type>
                                                                             {
                                                                                 {CqlType.Ascii, typeof (string)},
                                                                                 {CqlType.Text, typeof (string)},
                                                                                 {CqlType.Varchar, typeof (string)},
                                                                                 {CqlType.Blob, typeof (byte[])},
                                                                                 {CqlType.Double, typeof (double)},
                                                                                 {CqlType.Float, typeof (float)},
                                                                                 {CqlType.Decimal, typeof (decimal)},
                                                                                 {CqlType.Bigint, typeof (long)},
                                                                                 {CqlType.Counter, typeof (long)},
                                                                                 {CqlType.Int, typeof (int)},
                                                                                 {CqlType.Boolean, typeof (bool)},
                                                                                 {CqlType.Uuid, typeof (Guid)},
                                                                                 {CqlType.Timeuuid, typeof (Guid)},
                                                                                 {CqlType.Inet, typeof (IPAddress)},
                                                                                 {CqlType.Varint, typeof (BigInteger)},
                                                                                 {CqlType.Timestamp, typeof (DateTime)}
                                                                             };

        private static readonly Dictionary<Type, CqlType> Type2CqlType = new Dictionary<Type, CqlType>
                                                                             {
                                                                                 {typeof (string), CqlType.Varchar},
                                                                                 {typeof (byte[]), CqlType.Blob},
                                                                                 {typeof (double), CqlType.Double},
                                                                                 {typeof (float), CqlType.Float},
                                                                                 {typeof (decimal), CqlType.Decimal},
                                                                                 {typeof (long), CqlType.Bigint},
                                                                                 {typeof (int), CqlType.Int},
                                                                                 {typeof (bool), CqlType.Boolean},
                                                                                 {typeof (Guid), CqlType.Uuid},
                                                                                 {typeof (IPAddress), CqlType.Inet},
                                                                                 {typeof (BigInteger), CqlType.Varint},
                                                                                 {typeof (DateTime), CqlType.Timestamp}
                                                                             };

        private static readonly Dictionary<CqlType, DbType> CqlType2DbType = new Dictionary<CqlType, DbType>
                                                                                 {
                                                                                     {CqlType.Ascii, DbType.AnsiString},
                                                                                     {CqlType.Text, DbType.String},
                                                                                     {CqlType.Varchar, DbType.String},
                                                                                     {CqlType.Blob, DbType.Binary},
                                                                                     {CqlType.Double, DbType.Double},
                                                                                     {CqlType.Float, DbType.Single},
                                                                                     {CqlType.Decimal, DbType.Decimal},
                                                                                     {CqlType.Bigint, DbType.Int64},
                                                                                     {CqlType.Counter, DbType.Int64},
                                                                                     {CqlType.Int, DbType.Int32},
                                                                                     {CqlType.Boolean, DbType.Boolean},
                                                                                     {CqlType.Uuid, DbType.Guid},
                                                                                     {CqlType.Timeuuid, DbType.Guid},
                                                                                     {CqlType.Varint, DbType.VarNumeric},
                                                                                     {CqlType.Timestamp, DbType.DateTime}
                                                                                 };

        private static readonly Dictionary<DbType, CqlType> DbType2CqlType = new Dictionary<DbType, CqlType>
                                                                                 {
                                                                                     {DbType.AnsiString, CqlType.Ascii},
                                                                                     {DbType.Int64, CqlType.Bigint},
                                                                                     {DbType.Guid, CqlType.Uuid},
                                                                                     {DbType.Binary, CqlType.Blob},
                                                                                     {DbType.DateTime, CqlType.Timestamp},
                                                                                     {DbType.Single, CqlType.Float},
                                                                                     {DbType.Double, CqlType.Double},
                                                                                     {DbType.Decimal, CqlType.Decimal},
                                                                                     {DbType.Int32, CqlType.Int},
                                                                                     {DbType.Boolean, CqlType.Boolean},
                                                                                     {DbType.VarNumeric, CqlType.Varint},
                                                                                     {DbType.String, CqlType.Varchar},
                                                                                 };

        /// <summary>
        ///   Gets the .Net type that represents the given CqlType
        /// </summary>
        /// <param name="cqlType"> CqlType of the CQL. </param>
        /// <param name="valueType"> CqlType of the values if the CqlType is a collection. </param>
        /// <param name="keyType"> CqlType of the key if the type is a map. </param>
        /// <returns> .NET type representing the CqlType </returns>
        /// <exception cref="System.ArgumentException">Unsupported type</exception>
        public static Type ToType(this CqlType cqlType, CqlType? keyType = null, CqlType? valueType = null)
        {
            Type type;
            switch (cqlType)
            {
                case CqlType.Map:
                    Type genericMapType = typeof(Dictionary<,>);

                    Debug.Assert(keyType.HasValue, "a map should have a Key type");
                    Debug.Assert(valueType.HasValue, "a map should have a Value type");

                    type = genericMapType.MakeGenericType(keyType.Value.ToType(),
                                                          valueType.Value.ToType());
                    break;

                case CqlType.Set:
                    Type genericSetType = typeof(HashSet<>);
                    Debug.Assert(valueType.HasValue, "a set should have a Value type");

                    type = genericSetType.MakeGenericType(valueType.Value.ToType());
                    break;

                case CqlType.List:
                    Type genericListType = typeof(List<>);
                    Debug.Assert(valueType.HasValue, "a list should have a Value type");

                    type = genericListType.MakeGenericType(valueType.Value.ToType());
                    break;

                default:
                    if (!CqlType2Type.TryGetValue(cqlType, out type))
                        throw new ArgumentException("Unsupported type");
                    break;
            }

            return type;
        }

        /// <summary>
        /// Gets the corresponding the CqlType.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        /// <exception cref="System.NotSupportedException">CqlType can not be mapped to a valid CQL type</exception>
        public static CqlType ToCqlType(this Type type)
        {
            CqlType cqlType;

            if (!Type2CqlType.TryGetValue(type, out cqlType))
            {
                if (type.IsGenericType)
                {
                    var genericType = type.GetGenericTypeDefinition();

                    if (genericType == typeof(Nullable<>))
                        return ToCqlType(type.GetGenericArguments()[0]);

                    if (genericType == typeof(Dictionary<,>))
                        return CqlType.Map;

                    if (genericType == typeof(HashSet<>))
                        return CqlType.Set;

                    if (genericType == typeof(List<>))
                        return CqlType.List;

                }

                throw new NotSupportedException("CqlType " + type.Name + " can not be mapped to a valid CQL type");
            }

            return cqlType;
        }

        /// <summary>
        /// Determines whether the specified type is a supported CQL type
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        /// <exception cref="System.NotSupportedException">CqlType can not be mapped to a valid CQL type</exception>
        public static bool IsSupportedCqlType(this Type type)
        {
            if (Type2CqlType.ContainsKey(type))
                return true;

            if (type.IsGenericType)
            {
                Type genericType = type.GetGenericTypeDefinition();
                Type[] typeArguments = type.GetGenericArguments();

                if (genericType == typeof(Nullable<>))
                {
                    return Type2CqlType.ContainsKey(typeArguments[0]);
                }

                if (genericType == typeof(Dictionary<,>))
                {
                    var keyType = typeArguments[0];
                    var valueType = typeArguments[1];
                    return Type2CqlType.ContainsKey(keyType) && Type2CqlType.ContainsKey(valueType);
                }

                if (genericType == typeof(HashSet<>) || genericType == typeof(List<>))
                {
                    var collectionType = typeArguments[0];
                    return Type2CqlType.ContainsKey(collectionType);
                }


            }

            return true;
        }

        /// <summary>
        ///   gets the corresponding the DbType
        /// </summary>
        /// <param name="colType"> CqlType of the col. </param>
        /// <returns> </returns>
        public static DbType ToDbType(this CqlType colType)
        {
            DbType type;

            if (CqlType2DbType.TryGetValue(colType, out type))
            {
                return type;
            }


            return DbType.Object;
        }

        /// <summary>
        ///   gets the corresponding the CqlType
        /// </summary>
        /// <param name="colType"> CqlType of the col. </param>
        /// <returns> </returns>
        /// <exception cref="System.ArgumentOutOfRangeException">cqlType;DbType is not supported</exception>
        public static CqlType ToCqlType(this DbType colType)
        {
            CqlType type;

            if (DbType2CqlType.TryGetValue(colType, out type))
            {
                return type;
            }

            throw new ArgumentOutOfRangeException("colType", colType, "DbType is not supported");
        }
    }
}