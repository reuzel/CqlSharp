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

        public static byte[] Serialize(CqlType column, object data)
        {
            //null value check
            if (data == null || data == DBNull.Value)
                return null;

            byte[] rawData;
            switch (column.CqlTypeCode)
            {
                case CqlTypeCode.List:
                case CqlTypeCode.Set:
                    var coll = (IEnumerable)data;
                    using (var ms = new MemoryStream())
                    {
                        //write length placeholder
                        ms.Position = 2;
                        ushort count = 0;
                        foreach (object elem in coll)
                        {
                            byte[] rawDataElem = Serialize(column.CollectionValueType, elem);
                            ms.WriteShortByteArray(rawDataElem);
                            count++;
                        }
                        ms.Position = 0;
                        ms.WriteShort(count);
                        rawData = ms.ToArray();
                    }
                    break;

                case CqlTypeCode.Map:
                    var map = (IDictionary)data;
                    using (var ms = new MemoryStream())
                    {
                        ms.WriteShort((ushort)map.Count);
                        foreach (DictionaryEntry de in map)
                        {
                            byte[] rawDataKey = Serialize(column.CollectionKeyType, de.Key);
                            ms.WriteShortByteArray(rawDataKey);
                            byte[] rawDataValue = Serialize(column.CollectionValueType, de.Value);
                            ms.WriteShortByteArray(rawDataValue);
                        }
                        rawData = ms.ToArray();
                    }
                    break;

                default:
                    rawData = Serialize(column.CqlTypeCode, data);
                    break;
            }

            return rawData;
        }

        public static byte[] Serialize(CqlTypeCode typeCode, object data)
        {
            //null value check
            if (data == null || data == DBNull.Value)
                return null;

            byte[] rawData;
            switch (typeCode)
            {
                case CqlTypeCode.Ascii:
                    rawData = Encoding.ASCII.GetBytes(Convert.ToString(data));
                    break;

                case CqlTypeCode.Text:
                case CqlTypeCode.Varchar:
                    rawData = Encoding.UTF8.GetBytes(Convert.ToString(data));
                    break;

                case CqlTypeCode.Blob:
                    rawData = (byte[])data;
                    break;

                case CqlTypeCode.Double:
                    rawData = BitConverter.GetBytes(Convert.ToDouble(data));
                    if (IsLittleEndian) Array.Reverse(rawData);
                    break;

                case CqlTypeCode.Float:
                    rawData = BitConverter.GetBytes(Convert.ToSingle(data));
                    if (IsLittleEndian) Array.Reverse(rawData);
                    break;

                case CqlTypeCode.Timestamp:
                    if (data is long)
                        rawData = BitConverter.GetBytes((long)data);
                    else
                        rawData = BitConverter.GetBytes(Convert.ToDateTime(data).ToTimestamp());

                    if (IsLittleEndian) Array.Reverse(rawData);

                    break;

                case CqlTypeCode.Bigint:
                case CqlTypeCode.Counter:
                    rawData = BitConverter.GetBytes(Convert.ToInt64(data));
                    if (IsLittleEndian) Array.Reverse(rawData);
                    break;

                case CqlTypeCode.Int:
                    rawData = BitConverter.GetBytes(Convert.ToInt32(data));
                    if (IsLittleEndian) Array.Reverse(rawData);
                    break;

                case CqlTypeCode.Varint:
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

                case CqlTypeCode.Boolean:
                    rawData = BitConverter.GetBytes(Convert.ToBoolean(data));
                    break;

                case CqlTypeCode.Uuid:
                case CqlTypeCode.Timeuuid:
                    var guid = (Guid)data;
                    rawData = guid.ToByteArray();
                    if (IsLittleEndian)
                    {
                        Array.Reverse(rawData, 0, 4);
                        Array.Reverse(rawData, 4, 2);
                        Array.Reverse(rawData, 6, 2);
                    }

                    break;

                case CqlTypeCode.Inet:
                    rawData = ((IPAddress)data).GetAddressBytes();
                    break;

                case CqlTypeCode.Decimal:
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
                    throw new ArgumentException("Unsupported typeCode");
            }

            return rawData;
        }

        public static object Deserialize(CqlType type, byte[] rawData)
        {
            //skip parsing and return null value when rawData is null
            if (rawData == null)
                return null;

            object data;
            switch (type.CqlTypeCode)
            {
                case CqlTypeCode.List:
                    data = DeserializeList(type.CollectionValueType, rawData);
                    break;

                case CqlTypeCode.Set:
                    data = DeserializeSet(type.CollectionValueType, rawData);
                    break;

                case CqlTypeCode.Map:
                    data = DeserializeMap(type.CollectionKeyType, type.CollectionValueType, rawData);
                    break;

                default:
                    data = Deserialize(type.CqlTypeCode, rawData);
                    break;
            }

            return data;
        }

        public static IDictionary DeserializeMap(CqlType collectionKeyType, CqlType collectionValueType, byte[] rawData)
        {
            Type typedDic = new CqlType(CqlTypeCode.Map, collectionKeyType, collectionValueType).ToType();
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
            Type typedSet = new CqlType(CqlTypeCode.Set, collectionValueType).ToType();
            return Activator.CreateInstance(typedSet, items);
        }

        public static IList DeserializeList(CqlType collectionValueType, byte[] rawData)
        {
            Type typedColl = new CqlType(CqlTypeCode.List, collectionValueType).ToType();
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

        private static object Deserialize(CqlTypeCode typeCode, byte[] rawData)
        {
            if (rawData == null)
                return null;

            object data;
            switch (typeCode)
            {
                case CqlTypeCode.Ascii:
                    data = DeserializeAsciiString(rawData);
                    break;

                case CqlTypeCode.Text:
                case CqlTypeCode.Varchar:
                    data = DeserializeUtfString(rawData);
                    break;

                case CqlTypeCode.Blob:
                    data = DeserializeBlob(rawData);
                    break;

                case CqlTypeCode.Double:
                    data = DeserializeDouble(rawData);
                    break;

                case CqlTypeCode.Float:
                    data = DeserializeFloat(rawData);
                    break;

                case CqlTypeCode.Timestamp:
                    data = DeserializeDateTime(rawData);
                    break;

                case CqlTypeCode.Bigint:
                case CqlTypeCode.Counter:
                    data = DeserializeLong(rawData);
                    break;

                case CqlTypeCode.Int:
                    data = DeserializeInt(rawData);
                    break;

                case CqlTypeCode.Varint:
                    data = DeserializeBigInteger(rawData);
                    break;

                case CqlTypeCode.Boolean:
                    data = DeserializeBoolean(rawData);
                    break;

                case CqlTypeCode.Uuid:
                case CqlTypeCode.Timeuuid:
                    return DeserializeGuid(rawData);

                case CqlTypeCode.Inet:
                    data = DeserializeIPAddress(rawData);
                    break;

                case CqlTypeCode.Decimal:
                    data = DeserializeDecimal(rawData);
                    break;

                default:
                    throw new ArgumentException("Unsupported typeCode");
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

        public static TUserType DeserializeUserType<TUserType>(byte[] rawData)
        {
            var userType = (TUserType)Activator.CreateInstance(typeof(TUserType));
            var accessor = ObjectAccessor<TUserType>.Instance;

            using (var ms = new MemoryStream(rawData))
            {
                foreach (var column in accessor.Columns)
                {
                    byte[] elemRawData = ms.ReadShortByteArray();
                    column.WriteFunction(userType, Deserialize(column.CqlType, elemRawData));
                    ms.ReadByte();
                }
            }

            return userType;
        }

    }
}