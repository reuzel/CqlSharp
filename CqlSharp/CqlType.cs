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
using System.Collections.Concurrent;
using System.Collections.Generic;
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

    internal static class CqlTypeExtensions
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


        public static Type ToType(this CqlType colType)
        {
            Type type;
            if (ColType2Type.TryGetValue(colType, out type))
            {
                return type;
            }

            throw new ArgumentException("Unsupported type");
        }
    }
}