// CqlSharp - CqlSharp.Test
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
using System.Text;
using CqlSharp.Serialization;
using CqlSharp.Serialization.Marshal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Test
{
    [TestClass]
    public class ConversionTest
    {
        [TestMethod]
        public void ConvertStringToInt()
        {
            const string source = "123";
            int value = Converter.ChangeType<string, int>(source);
            Assert.AreEqual(123, value);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ConvertStringWithNullValueToInt()
        {
            Converter.ChangeType<string, int>(null);
        }

        [TestMethod]
        public void ConvertStringToNullableInt()
        {
            const string source = "123";
            int? value = Converter.ChangeType<string, int?>(source);
            Assert.AreEqual(123, value);
        }

        [TestMethod]
        public void ConvertStringWithNullValueToNullableInt()
        {
            int? value = Converter.ChangeType<string, int?>(null);
            Assert.IsNull(value);
        }

        [TestMethod]
        public void ConvertIntToString()
        {
            const int source = 123;
            string value = Converter.ChangeType<int, string>(source);
            Assert.AreEqual("123", value);
        }

        [TestMethod]
        public void ConvertNullableIntToString()
        {
            int? source = 123;
            string value = Converter.ChangeType<int?, string>(source);
            Assert.AreEqual("123", value);
        }

        [TestMethod]
        public void ConvertNullableIntWithNullValueToString()
        {
            string value = Converter.ChangeType<int?, string>(null);
            Assert.AreEqual("", value); //nullables with null values are translated to empty strings by default
        }

        [TestMethod]
        public void ConvertLongToInt()
        {
            const long source = 123L;
            int value = Converter.ChangeType<long, int>(source);
            Assert.AreEqual(123, value);
        }

        [TestMethod]
        public void ConvertLongToNullableInt()
        {
            const long source = 123L;
            int? value = Converter.ChangeType<long, int?>(source);

            Assert.AreEqual(123, value);
        }

        [TestMethod]
        public void ConvertNullableLongToNullableInt()
        {
            long? source = 123L;
            int? value = Converter.ChangeType<long?, int?>(source);

            Assert.AreEqual(123, value);
        }

        [TestMethod]
        public void ConvertNullableLongToInt()
        {
            long? source = 123L;
            int value = Converter.ChangeType<long?, int>(source);

            Assert.AreEqual(123, value);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void ConvertNullableLongWithNullValueToInt()
        {
            int value = Converter.ChangeType<long?, int>(null);
            Assert.AreEqual(0, value);
        }

        [TestMethod]
        public void ConvertNullableLongWithNullValueToNullableInt()
        {
            int? value = Converter.ChangeType<long?, int?>(null);
            Assert.IsNull(value);
        }

        [TestMethod]
        public void ConvertListIntToSetInt()
        {
            var source = new List<int> {1, 2, 3};
            HashSet<int> value = Converter.ChangeType<List<int>, HashSet<int>>(source);

            Assert.AreEqual(3, value.Count);
        }

        [TestMethod]
        public void ConvertListIntToArrayInt()
        {
            var source = new List<int> { 1, 2, 3 };
            int[] value = Converter.ChangeType<List<int>, int[]>(source);

            Assert.AreEqual(3, value.Length);
        }

        [TestMethod]
        public void ConvertArrayIntToListInt()
        {
            var source = new int[] { 1, 2, 3 };
            List<int> value = Converter.ChangeType<int[], List<int>>(source);

            Assert.AreEqual(3, value.Count);
        }

        [TestMethod]
        public void ConvertListIntWithDoubleEntriesToSetInt()
        {
            var source = new List<int> {1, 2, 2, 3, 3};
            HashSet<int> value = Converter.ChangeType<List<int>, HashSet<int>>(source);

            Assert.AreEqual(3, value.Count);
        }

        [TestMethod]
        public void ConvertListIntToSetbyte()
        {
            var source = new List<int> {1, 2, 2, 3, 3};
            var value = Converter.ChangeType<List<int>, HashSet<byte>>(source);

            Assert.IsInstanceOfType(value, typeof(HashSet<byte>));
            Assert.AreEqual(3, value.Count);
        }

        [TestMethod]
        public void ConvertBytesToMyCustomType()
        {
            const string str = "hallo wereld!";
            var source = Encoding.UTF32.GetBytes(str);

            var value = Converter.ChangeType<byte[], MyCustomType>(source);

            Assert.IsInstanceOfType(value, typeof(MyCustomType));
            Assert.AreEqual(str, value.MyString);
        }

        [TestMethod]
        public void ConvertTuple()
        {
            var t1 = Tuple.Create("123", 100, (short)200);

            var value = Converter.ChangeType<Tuple<string, int, short>, Tuple<int, string, byte>>(t1);

            Assert.AreEqual(123, value.Item1);
            Assert.AreEqual("100", value.Item2);
            Assert.AreEqual((byte)200, value.Item3);
        }

        [TestMethod]
        public void ConvertTupleToShorterTuple()
        {
            var t1 = Tuple.Create("123", 100, (short)200);

            var value = Converter.ChangeType<Tuple<string, int, short>, Tuple<int, string>>(t1);

            Assert.AreEqual(123, value.Item1);
            Assert.AreEqual("100", value.Item2);
        }

        [TestMethod]
        public void ConvertDictionary()
        {
            var t1 = new Dictionary<string, int>
            {
                {"123", 1},
                {"456", 2}
            };

            var value = Converter.ChangeType<Dictionary<string, int>, Dictionary<int, string>>(t1);

            Assert.IsTrue(value.ContainsKey(123));
            Assert.AreEqual("2", value[456]);
        }


        [CqlCustomType(typeof(BytesTypeFactory))]
        [CqlTypeConverter(typeof(MyCustomTypeConverter))]
        private class MyCustomType
        {
            private class MyCustomTypeConverter : ITypeConverter<MyCustomType>
            {
                /// <summary>
                /// Converts the source object to an object of the the given target type.
                /// </summary>
                /// <typeparam name="TTarget">The type of the target.</typeparam>
                /// <param name="source">The source.</param>
                /// <returns>an object of the the given target type</returns>
                public TTarget ConvertTo<TTarget>(MyCustomType source)
                {
                    if(typeof(TTarget) != typeof(byte[]))
                        throw new InvalidCastException("Can't convert MyCustomType to anything else but byte[]");

                    return (TTarget)(object)Encoding.UTF32.GetBytes(source.MyString);
                }

                /// <summary>
                /// Converts an object of the given source type to an instance of this converters type
                /// </summary>
                /// <typeparam name="TSource">The type of the source.</typeparam>
                /// <param name="source">The source.</param>
                /// <returns></returns>
                public MyCustomType ConvertFrom<TSource>(TSource source)
                {
                    if(typeof(TSource) != typeof(byte[]))
                        throw new InvalidCastException("Can't convert to MyCustomType from anything else but byte[]");

                    var data = source as byte[];
                    if(data != null)
                        return new MyCustomType {MyString = Encoding.UTF32.GetString(data)};

                    return null;
                }
            }

            public string MyString { get; private set; }
        }
    }
}