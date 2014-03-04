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

using CqlSharp.Protocol;
using System;
using System.Collections;
using System.IO;
using System.Net;
using System.Numerics;
using System.Text;

namespace CqlSharp.Serialization
{
    /// <summary>
    ///   Implements (de)serialization of values based on column specifications
    /// </summary>
    /// <remarks>
    ///   Based on code from <a href="https://github.com/pchalamet/cassandra-sharp">Casssandra-Sharp</a> project.
    /// </remarks>
    internal static class ValueSerialization
    {
        private static readonly bool IsLittleEndian = BitConverter.IsLittleEndian;

        public static byte[] Serialize(CqlType type, CqlType? collectionKeyType, CqlType? collectionValueType,
                                       object data)
        {
            //null value check
            if (data == null || data == DBNull.Value)
                return null;

            byte[] rawData;
            switch (type)
            {
                case CqlType.List:
                case CqlType.Set:
                    if (!collectionValueType.HasValue)
                        throw new CqlException("Column collection type must has its value type set");

                    var coll = (IEnumerable)data;
                    using (var ms = new MemoryStream())
                    {
                        //write length placeholder
                        ms.Position = 2;
                        ushort count = 0;
                        foreach (object elem in coll)
                        {
                            byte[] rawDataElem = Serialize(collectionValueType.Value, elem);
                            ms.WriteShortByteArray(rawDataElem);
                            count++;
                        }
                        ms.Position = 0;
                        ms.WriteShort(count);
                        rawData = ms.ToArray();
                    }
                    break;

                case CqlType.Map:

                    if (!collectionKeyType.HasValue)
                        throw new CqlException("Column map type must has its key type set");

                    if (!collectionValueType.HasValue)
                        throw new CqlException("Column map type must has its value type set");

                    var map = (IDictionary)data;
                    using (var ms = new MemoryStream())
                    {
                        ms.WriteShort((ushort)map.Count);
                        foreach (DictionaryEntry de in map)
                        {
                            byte[] rawDataKey = Serialize(collectionKeyType.Value, de.Key);
                            ms.WriteShortByteArray(rawDataKey);
                            byte[] rawDataValue = Serialize(collectionValueType.Value, de.Value);
                            ms.WriteShortByteArray(rawDataValue);
                        }
                        rawData = ms.ToArray();
                    }
                    break;

                default:
                    rawData = Serialize(type, data);
                    break;
            }

            return rawData;
        }

        public static byte[] Serialize(CqlType type, object data)
        {
            //null value check
            if (data == null || data == DBNull.Value)
                return null;

            byte[] rawData;
            switch (type)
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
                    rawData = guid.ToByteArray();
                    if (IsLittleEndian)
                    {
                        Array.Reverse(rawData, 0, 4);
                        Array.Reverse(rawData, 4, 2);
                        Array.Reverse(rawData, 6, 2);
                    }

                    break;

                case CqlType.Inet:
                    rawData = ((IPAddress)data).GetAddressBytes();
                    break;

                case CqlType.Decimal:
                    //get binary representation of the decimal
                    int[] bits = decimal.GetBits((decimal)data);

                    //extract the sign
                    bool sign = (bits[3] & 0x80000000) != 0;

                    //construct the (signed) unscaled value
                    BigInteger unscaled = (uint)bits[2];
                    unscaled = (unscaled << 32) + (uint)bits[1];
                    unscaled = (unscaled << 32) + (uint)bits[0];
                    if (sign) unscaled *= -1;

                    //get the unscaled value binary representation (Little Endian)
                    var unscaledData = unscaled.ToByteArray();

                    //construct the result array
                    rawData = new byte[4 + unscaledData.Length];

                    //copy the scale into the rawData
                    int scale = (bits[3] >> 16) & 0x7F;
                    rawData[0] = (byte)(scale >> 24);
                    rawData[1] = (byte)(scale >> 16);
                    rawData[2] = (byte)(scale >> 8);
                    rawData[3] = (byte)(scale);

                    //copy the unscaled value (Big Endian)
                    for (int i = 0; i < unscaledData.Length; i++)
                        rawData[i + 4] = unscaledData[unscaledData.Length - 1 - i];

                    break;

                default:
                    throw new ArgumentException("Unsupported type");
            }

            return rawData;
        }

        public static object Deserialize(CqlType type, CqlType? collectionKeyType, CqlType? collectionValueType,
                                         byte[] rawData)
        {
            //skip parsing and return null value when rawData is null
            if (rawData == null)
                return null;

            object data;
            switch (type)
            {
                default:
                    data = Deserialize(type, rawData);
                    break;

                case CqlType.List:
                    if (!collectionValueType.HasValue)
                        throw new CqlException("Can't deserialize a list without a list content type");

                    data = DeserializeList(collectionValueType.Value, rawData);
                    break;

                case CqlType.Set:
                    if (!collectionValueType.HasValue)
                        throw new CqlException("Can't deserialize a set without a set content type");

                    data = DeserializeSet(collectionValueType.Value, rawData);
                    break;

                case CqlType.Map:
                    if (!collectionKeyType.HasValue)
                        throw new CqlException("Column map type must has its key type set");

                    if (!collectionValueType.HasValue)
                        throw new CqlException("Column map type must has its value type set");

                    data = DeserializeMap(collectionKeyType.Value, collectionValueType.Value, rawData);
                    break;
            }

            return data;
        }

        public static IDictionary DeserializeMap(CqlType collectionKeyType, CqlType collectionValueType, byte[] rawData)
        {
            Type typedDic = CqlType.Map.ToType(collectionKeyType, collectionValueType);
            var map = (IDictionary)Activator.CreateInstance(typedDic);
            using (var ms = new MemoryStream(rawData))
            {
                ushort nbElem = ms.ReadShort();
                for (int i = 0; i < nbElem; i++)
                {
                    byte[] elemRawKey = ms.ReadShortByteArray();
                    byte[] elemRawValue = ms.ReadShortByteArray();
                    object key = Deserialize(collectionKeyType, elemRawKey);
                    object value = Deserialize(collectionValueType, elemRawValue);
                    map.Add(key, value);
                }
            }
            return map;
        }

        public static object DeserializeSet(CqlType collectionValueType, byte[] rawData)
        {
            IList items = DeserializeList(collectionValueType, rawData);
            Type typedSet = CqlType.Set.ToType(null, collectionValueType);
            return Activator.CreateInstance(typedSet, items);
        }

        public static IList DeserializeList(CqlType collectionValueType, byte[] rawData)
        {
            Type typedColl = CqlType.List.ToType(null, collectionValueType);
            var list = (IList)Activator.CreateInstance(typedColl);
            using (var ms = new MemoryStream(rawData))
            {
                ushort nbElem = ms.ReadShort();
                for (int i = 0; i < nbElem; i++)
                {
                    byte[] elemRawData = ms.ReadShortByteArray();
                    object elem = Deserialize(collectionValueType, elemRawData);
                    list.Add(elem);
                }
            }
            return list;
        }

        internal static object Deserialize(CqlType type, byte[] rawData)
        {
            if (rawData == null)
                return null;

            object data;
            switch (type)
            {
                case CqlType.Ascii:
                    data = DeserializeAsciiString(rawData);
                    break;

                case CqlType.Text:
                case CqlType.Varchar:
                    data = DeserializeUtfString(rawData);
                    break;

                case CqlType.Blob:
                    data = DeserializeBlob(rawData);
                    break;

                case CqlType.Double:
                    data = DeserializeDouble(rawData);
                    break;

                case CqlType.Float:
                    data = DeserializeFloat(rawData);
                    break;

                case CqlType.Timestamp:
                    data = DeserializeDateTime(rawData);
                    break;

                case CqlType.Bigint:
                case CqlType.Counter:
                    data = DeserializeLong(rawData);
                    break;

                case CqlType.Int:
                    data = DeserializeInt(rawData);
                    break;

                case CqlType.Varint:
                    data = DeserializeBigInteger(rawData);
                    break;

                case CqlType.Boolean:
                    data = DeserializeBoolean(rawData);
                    break;

                case CqlType.Uuid:
                case CqlType.Timeuuid:
                    return DeserializeGuid(rawData);

                case CqlType.Inet:
                    data = DeserializeIPAddress(rawData);
                    break;

                case CqlType.Decimal:
                    data = DeserializeDecimal(rawData);
                    break;

                default:
                    throw new ArgumentException("Unsupported type");
            }

            return data;
        }

        public static decimal DeserializeDecimal(byte[] rawData)
        {
            //extract scale
            int scale = rawData.ToInt();

            //check the scale if it ain't too large (or small)
            if (scale < 0 || scale > 28)
                throw new CqlException("Received decimal is too large to fit in a System.Decimal");

            //copy the unscaled big integer data (and reverse to Little Endian)
            var unscaledData = new byte[rawData.Length - 4];
            for (int i = 0; i < unscaledData.Length; i++)
                unscaledData[i] = rawData[rawData.Length - 1 - i];

            //get the unscaled value
            var unscaled = new BigInteger(unscaledData);

            //get the sign, and make sure unscaled data is positive
            bool sign = unscaled < 0;
            if (sign) unscaled *= -1;

            //check unscaled size (Java BigDecimal can be larger the System.Decimal)
            if ((unscaled >> 96) != 0)
                throw new CqlException("Received decimal is too large to fit in a System.Decimal");

            //get the decimal int values
            var low = (uint)(unscaled & 0xFFFFFFFF);
            var mid = (uint)((unscaled >> 32) & 0xFFFFFFFF);
            var high = (uint)((unscaled >> 64) & 0xFFFFFFFF);

            //construct the decimal
            return new decimal((int)low, (int)mid, (int)high, sign, (byte)scale);
        }

        public static IPAddress DeserializeIPAddress(byte[] rawData)
        {
            return new IPAddress(rawData);
        }

        public static Guid DeserializeGuid(byte[] rawData)
        {
            return rawData.ToGuid();
        }

        public static bool DeserializeBoolean(byte[] rawData)
        {
            return BitConverter.ToBoolean(rawData, 0);
        }

        public static BigInteger DeserializeBigInteger(byte[] rawData)
        {
            //to little endian
            Array.Reverse(rawData);
            return new BigInteger(rawData);
        }

        public static int DeserializeInt(byte[] rawData)
        {
            return rawData.ToInt();
        }

        public static long DeserializeLong(byte[] rawData)
        {
            return rawData.ToLong();
        }

        public static DateTime DeserializeDateTime(byte[] rawData)
        {
            return rawData.ToLong().ToDateTime();
        }

        public static float DeserializeFloat(byte[] rawData)
        {
            if (IsLittleEndian) Array.Reverse(rawData);
            return BitConverter.ToSingle(rawData, 0);
        }

        public static double DeserializeDouble(byte[] rawData)
        {
            if (IsLittleEndian) Array.Reverse(rawData);
            return BitConverter.ToDouble(rawData, 0);
        }

        public static string DeserializeUtfString(byte[] rawData)
        {
            return Encoding.UTF8.GetString(rawData);
        }

        public static string DeserializeAsciiString(byte[] rawData)
        {
            return Encoding.ASCII.GetString(rawData);
        }

        public static byte[] DeserializeBlob(byte[] currentValue)
        {
            return currentValue;
        }
    }
}