using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace CqlSharp.Test
{
    [TestClass]
    public class TypeParserTest
    {
        [TestMethod]
        public void ParseSimpleAscii()
        {
            const string typeName = "AsciiType";

            CqlType type = CqlType.CreateType(typeName);

            Assert.AreEqual(CqlType.Ascii, type);
        }

        [TestMethod]
        public void ParseAsciiList()
        {
            const string typeName = "ListType(AsciiType)";

            CqlType type = CqlType.CreateType(typeName);

            Assert.AreEqual(CqlType.CreateType(CqlTypeCode.List, CqlType.Ascii), type);
        }

        [TestMethod]
        public void ParseUUIDVarcharMap()
        {
            const string typeName = "MapType(UUIDType,UTF8Type)";
            CqlType expected = CqlType.CreateType(CqlTypeCode.Map, CqlType.Uuid, CqlType.Varchar);

            CqlType type = CqlType.CreateType(typeName);
            
            Assert.AreEqual(expected, type);
        }

        [TestMethod]
        public void CreateDictionaryType()
        {
            
            CqlType expected = CqlType.CreateType(CqlTypeCode.Map, CqlType.Uuid, CqlType.Varchar);

            CqlType type = CqlType.CreateType(typeof(Dictionary<Guid, string>));

            Assert.AreEqual(expected, type);
        }


    }
}
