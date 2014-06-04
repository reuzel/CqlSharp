using CqlSharp.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;

namespace CqlSharp.Test
{
    [TestClass]
    public class ConversionTest
    {

        [TestMethod]
        public void ConvertStringToInt()
        {
            var source = "123";
            int value = Converter.ChangeType<string, int>(source);
            Assert.AreEqual(123, value);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ConvertStringWithNullValueToInt()
        {
            string source = null;
            int value = Converter.ChangeType<string, int>(source);
        }

        [TestMethod]
        public void ConvertStringToNullableInt()
        {
            var source = "123";
            int? value = Converter.ChangeType<string, int?>(source);
            Assert.AreEqual(123, value);
        }

        [TestMethod]
        public void ConvertStringWithNullValueToNullableInt()
        {
            string source = null;
            int? value = Converter.ChangeType<string, int?>(source);
            Assert.IsNull(value);
        }

        [TestMethod]
        public void ConvertIntToString()
        {
            var source = 123;
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
            int? source = null;
            string value = Converter.ChangeType<int?, string>(source);
            Assert.AreEqual("", value); //nullables with null values are translated to empty strings by default
        }

        [TestMethod]
        public void ConvertLongToInt()
        {
            var source = 123L;
            int value = Converter.ChangeType<long, int>(source);
            Assert.AreEqual(123, value);
        }

        [TestMethod]
        public void ConvertLongToNullableInt()
        {
            var source = 123L;
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
            long? source = null;
            int value = Converter.ChangeType<long?, int>(source);
        }

        [TestMethod]
        public void ConvertNullableLongWithNullValueToNullableInt()
        {
            long? source = null;
            int? value = Converter.ChangeType<long?, int?>(source);

            Assert.IsNull(value);
        }

        [TestMethod]
        public void ConvertListIntToSetInt()
        {
            var source = new List<int> { 1, 2, 3 };
            HashSet<int> value = Converter.ChangeType<List<int>, HashSet<int>>(source);

            Assert.AreEqual(3, value.Count);
        }

        [TestMethod]
        public void ConvertListIntWithDoubleEntriesToSetInt()
        {
            var source = new List<int> { 1, 2, 2, 3, 3 };
            HashSet<int> value = Converter.ChangeType<List<int>, HashSet<int>>(source);

            Assert.AreEqual(3, value.Count);
        }
    }
}
