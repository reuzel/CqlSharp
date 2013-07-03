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
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Numerics;
using System.Text;

namespace CqlSharp.Protocol
{
    /// <summary>
    ///   Implements (de)serialization of values based on column specifications
    /// </summary>
    /// <remarks>
    ///   Based on code from <a href="https://github.com/pchalamet/cassandra-sharp">Casssandra-Sharp</a> project.
    /// </remarks>
    internal static class ValueSerialization
    {
        private static readonly Dictionary<CqlType, Type> ColType2Type = new Dictionary<CqlType, Type>
                                                                             {
                                                                                 {CqlType.Ascii, typeof (string)},
                                                                                 {CqlType.Text, typeof (string)},
                                                                                 {CqlType.Varchar, typeof (string)},
                                                                                 {CqlType.Blob, typeof (byte[])},
                                                                                 {CqlType.Double, typeof (double)},
                                                                                 {CqlType.Float, typeof (float)},
                                                                                 {CqlType.Bigint, typeof (long)},
                                                                                 {CqlType.Counter, typeof (long)},
                                                                                 {CqlType.Int, typeof (int)},
                                                                                 {CqlType.Boolean, typeof (bool)},
                                                                                 {CqlType.Uuid, typeof (Guid)},
                                                                                 {CqlType.Timeuuid, typeof (Guid)},
                                                                                 {CqlType.Inet, typeof (IPAddress)},
                                                                                 {CqlType.Varint, typeof (BigInteger)},
                                                                                 {CqlType.Timestamp, typeof (DateTime)},
                                                                                 {CqlType.List, typeof (List<>)},
                                                                                 {CqlType.Set, typeof (HashSet<>)},
                                                                                 {CqlType.Map, typeof (Dictionary<,>)}
                                                                             };

        private static readonly ConcurrentDictionary<CqlType, object> TypeDefaults =
            new ConcurrentDictionary<CqlType, object>();

        private static readonly bool IsLittleEndian = BitConverter.IsLittleEndian;

        public static byte[] Serialize(this CqlColumn cqlColumn, object data)
        {
            //null value check
            if (data == null)
                return null;

            byte[] rawData;
            switch (cqlColumn.CqlType)
            {
                case CqlType.List:
                case CqlType.Set:
                    if (!cqlColumn.CollectionValueType.HasValue)
                        throw new CqlException("CqlColumn collection type must has its value type set");

                    var coll = (IEnumerable)data;
                    using (var ms = new MemoryStream())
                    {
                        //write length placeholder
                        ms.Position = 2;
                        short count = 0;
                        foreach (object elem in coll)
                        {
                            byte[] rawDataElem = Serialize(cqlColumn.CollectionValueType.Value, elem);
                            ms.WriteShortByteArray(rawDataElem);
                            count++;
                        }
                        ms.Position = 0;
                        ms.WriteShort(count);
                        rawData = ms.ToArray();
                    }
                    break;

                case CqlType.Map:

                    if (!cqlColumn.CollectionKeyType.HasValue)
                        throw new CqlException("CqlColumn map type must has its key type set");

                    if (!cqlColumn.CollectionValueType.HasValue)
                        throw new CqlException("CqlColumn map type must has its value type set");

                    var map = (IDictionary)data;
                    using (var ms = new MemoryStream())
                    {
                        ms.WriteShort((short)map.Count);
                        foreach (DictionaryEntry de in map)
                        {
                            byte[] rawDataKey = Serialize(cqlColumn.CollectionKeyType.Value, de.Key);
                            ms.WriteShortByteArray(rawDataKey);
                            byte[] rawDataValue = Serialize(cqlColumn.CollectionValueType.Value, de.Value);
                            ms.WriteShortByteArray(rawDataValue);
                        }
                        rawData = ms.ToArray();
                    }
                    break;

                default:
                    rawData = Serialize(cqlColumn.CqlType, data);
                    break;
            }

            return rawData;
        }

        public static byte[] Serialize(CqlType colType, object data)
        {
            //null value check
            if (data == null)
                return null;

            byte[] rawData;
            switch (colType)
            {
                case CqlType.Ascii:
                    rawData = Encoding.ASCII.GetBytes(Convert.ToString(data));
                    break;

                case CqlType.Text:
                case CqlType.Varchar:
                    rawData = Encoding.UTF8.GetBytes(Convert.ToString(data));
                    break;

                case CqlType.Blob:
                    rawData = (byte[])data;
                    break;

                case CqlType.Double:
                    rawData = BitConverter.GetBytes(Convert.ToDouble(data));
                    if (IsLittleEndian) Array.Reverse(rawData);
                    break;

                case CqlType.Float:
                    rawData = BitConverter.GetBytes(Convert.ToSingle(data));
                    if (IsLittleEndian) Array.Reverse(rawData);
                    break;

                case CqlType.Timestamp:
                    if (data is long)
                        rawData = BitConverter.GetBytes((long)data);
                    else
                        rawData = BitConverter.GetBytes(Convert.ToDateTime(data).ToTimestamp());

                    if (IsLittleEndian) Array.Reverse(rawData);

                    break;

                case CqlType.Bigint:
                case CqlType.Counter:
                    rawData = BitConverter.GetBytes(Convert.ToInt64(data));
                    if (IsLittleEndian) Array.Reverse(rawData);
                    break;

                case CqlType.Int:
                    rawData = BitConverter.GetBytes(Convert.ToInt32(data));
                    if (IsLittleEndian) Array.Reverse(rawData);
                    break;

                case CqlType.Varint:
                    var dataString = data as string;
                    if (dataString != null)
                        rawData = BigInteger.Parse(dataString).ToByteArray();
                    else
                    {
                        var integer = (BigInteger)data;
                        rawData = integer.ToByteArray();
                    }

                    //to bigendian
                    Array.Reverse(rawData);
                    break;

                case CqlType.Boolean:
                    rawData = BitConverter.GetBytes(Convert.ToBoolean(data));
                    break;

                case CqlType.Uuid:
                case CqlType.Timeuuid:
                    var guid = (Guid)data;

                    //return null if Guid is a nil Guid
                    if (guid == default(Guid))
                    {
                        rawData = null;
                    }
                    else
                    {
                        rawData = guid.ToByteArray();
                        if (IsLittleEndian)
                        {
                            Array.Reverse(rawData, 0, 4);
                            Array.Reverse(rawData, 4, 2);
                            Array.Reverse(rawData, 6, 2);
                        }
                    }

                    break;

                case CqlType.Inet:
                    rawData = ((IPAddress)data).GetAddressBytes();
                    break;

                default:
                    throw new ArgumentException("Unsupported type");
            }

            return rawData;
        }

        public static object Deserialize(this CqlColumn cqlColumn, byte[] rawData)
        {
            //skip parsing and return null value when rawData is null
            if (rawData == null)
                return GetNullValue(cqlColumn.CqlType);

            object data;
            Type colType;
            switch (cqlColumn.CqlType)
            {
                default:
                    data = Deserialize(cqlColumn.CqlType, rawData);
                    break;

                case CqlType.List:
                    if (!cqlColumn.CollectionValueType.HasValue)
                        throw new CqlException("CqlColumn collection type must has its value type set");

                    colType = cqlColumn.CollectionValueType.Value.ToType();
                    Type typedColl = typeof(List<>).MakeGenericType(colType);
                    var list = (IList)Activator.CreateInstance(typedColl);
                    using (var ms = new MemoryStream(rawData))
                    {
                        short nbElem = ms.ReadShort();
                        for (int i = 0; i < nbElem; i++)
                        {
                            byte[] elemRawData = ms.ReadShortByteArray();
                            object elem = Deserialize(cqlColumn.CollectionValueType.Value, elemRawData);
                            list.Add(elem);
                        }
                        data = list;
                    }
                    break;

                case CqlType.Set:
                    if (!cqlColumn.CollectionValueType.HasValue)
                        throw new CqlException("CqlColumn collection type must has its value type set");

                    colType = cqlColumn.CollectionValueType.Value.ToType();
                    Type tempListType = typeof(List<>).MakeGenericType(colType);
                    var tempList = (IList)Activator.CreateInstance(tempListType);
                    using (var ms = new MemoryStream(rawData))
                    {
                        short nbElem = ms.ReadShort();
                        for (int i = 0; i < nbElem; i++)
                        {
                            byte[] elemRawData = ms.ReadShortByteArray();
                            object elem = Deserialize(cqlColumn.CollectionValueType.Value, elemRawData);
                            tempList.Add(elem);
                        }

                        Type typedSet = typeof(HashSet<>).MakeGenericType(colType);
                        data = Activator.CreateInstance(typedSet, tempList);
                    }
                    break;

                case CqlType.Map:
                    if (!cqlColumn.CollectionKeyType.HasValue)
                        throw new CqlException("CqlColumn map type must has its key type set");

                    if (!cqlColumn.CollectionValueType.HasValue)
                        throw new CqlException("CqlColumn map type must has its value type set");

                    Type keyType = cqlColumn.CollectionKeyType.Value.ToType();
                    colType = cqlColumn.CollectionValueType.Value.ToType();
                    Type typedDic = typeof(Dictionary<,>).MakeGenericType(keyType, colType);
                    var dic = (IDictionary)Activator.CreateInstance(typedDic);
                    using (var ms = new MemoryStream(rawData))
                    {
                        short nbElem = ms.ReadShort();
                        for (int i = 0; i < nbElem; i++)
                        {
                            byte[] elemRawKey = ms.ReadShortByteArray();
                            byte[] elemRawValue = ms.ReadShortByteArray();
                            object key = Deserialize(cqlColumn.CollectionKeyType.Value, elemRawKey);
                            object value = Deserialize(cqlColumn.CollectionValueType.Value, elemRawValue);
                            dic.Add(key, value);
                        }
                        data = dic;
                    }
                    break;
            }

            return data;
        }

        private static object Deserialize(CqlType colType, byte[] rawData)
        {

            object data;
            switch (colType)
            {
                case CqlType.Ascii:
                    data = Encoding.ASCII.GetString(rawData);
                    break;

                case CqlType.Text:
                case CqlType.Varchar:
                    data = Encoding.UTF8.GetString(rawData);
                    break;

                case CqlType.Blob:
                    data = rawData;
                    break;

                case CqlType.Double:
                    if (IsLittleEndian) Array.Reverse(rawData);
                    data = BitConverter.ToDouble(rawData, 0);
                    break;

                case CqlType.Float:
                    if (IsLittleEndian) Array.Reverse(rawData);
                    data = BitConverter.ToSingle(rawData, 0);
                    break;

                case CqlType.Timestamp:
                    data = rawData.ToLong().ToDateTime();
                    break;

                case CqlType.Bigint:
                case CqlType.Counter:
                    data = rawData.ToLong();
                    break;

                case CqlType.Int:
                    data = rawData.ToInt();
                    break;

                case CqlType.Varint:
                    //to little endian
                    Array.Reverse(rawData);
                    data = new BigInteger(rawData);
                    break;

                case CqlType.Boolean:
                    data = BitConverter.ToBoolean(rawData, 0);
                    break;

                case CqlType.Uuid:
                case CqlType.Timeuuid:
                    return rawData.ToGuid();
                    break;

                case CqlType.Inet:
                    data = new IPAddress(rawData);
                    break;

                default:
                    throw new ArgumentException("Unsupported type");
            }

            return data;
        }

        private static Type ToType(this CqlType colType)
        {
            Type type;
            if (ColType2Type.TryGetValue(colType, out type))
            {
                return type;
            }

            throw new ArgumentException("Unsupported type");
        }



        /// <summary>
        /// Gets the c# null variant for the provided CqlType
        /// </summary>
        /// <param name="colType">a CqlType.</param>
        /// <returns>null if the CqlType is represented by a c# class, or Nullable if CqlType is represented by a struct</returns>
        private static object GetNullValue(this CqlType colType)
        {
            return TypeDefaults.GetOrAdd(colType, t =>
                                                       {
                                                           Type type = t.ToType();
                                                           if (type.IsValueType)
                                                           {
                                                               Type nullableGeneric = typeof(Nullable<>);
                                                               Type nullableType = nullableGeneric.MakeGenericType(type);
                                                               return Activator.CreateInstance(nullableType);
                                                           }

                                                           return null;
                                                       });


        }
    }
}