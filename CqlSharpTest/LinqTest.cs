using System;
using System.Collections.Generic;
using CqlSharp.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Diagnostics;
using System.Linq;

namespace CqlSharp.Test
{
    [TestClass]
    public class LinqTest
    {
        /// <summary>
        /// The context on which all queries are executed
        /// </summary>
        private static MyContext _context;

        /// <summary>
        /// class representing the values in a table
        /// </summary>
        public class MyValue
        {
            public int Id { get; set; }
            public string Value { get; set; }
        }

        /// <summary>
        /// The context used for testing
        /// </summary>
        public class MyContext : CqlContext
        {
            public CqlTable<MyValue> Values { get; set; }
        }

        /// <summary>
        /// Initializes the specified context.
        /// </summary>
        /// <param name="context">The context.</param>
        [ClassInitialize]
        public static void Init(TestContext context)
        {
            _context = new MyContext();
        }

        /// <summary>
        /// Cleanups this instance.
        /// </summary>
        [ClassCleanup]
        public static void Cleanup()
        {
            _context.Dispose();
        }

        /// <summary>
        /// Checks the result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="cql">The CQL.</param>
        private void CheckResult(IQueryable query, string cql)
        {
            Debug.WriteLine("Original:");
            Debug.WriteLine("\t{0}", query.Expression);

            Debug.WriteLine("Result:");
            var result = _context.ParseExpression(query.Expression);

            Debug.WriteLine("\t" + result.Item1);
            Debug.WriteLine("\t{0}", result.Item2);

            Assert.AreEqual(cql, result.Item1);
        }


        [TestMethod]
        public void WhereThenSelect()
        {
            var filter = "hallo";
            var query = _context.Values.Where(p => p.Value == filter + " daar").Select(r => r.Id);

            CheckResult(query, "SELECT 'id' FROM 'myvalue' WHERE 'value'='hallo daar';");
        }

        [TestMethod]
        public void SelectThenWhere()
        {
            var query = _context.Values.Select(r => r.Id).Where(id => id == 4);

            CheckResult(query, "SELECT 'id' FROM 'myvalue' WHERE 'id'=4;");
        }

        [TestMethod]
        public void NoWhereOrSelect()
        {
            var query = _context.Values;
            CheckResult(query, "SELECT 'id','value' FROM 'myvalue';");
        }

        [TestMethod]
        public void SelectAll()
        {
            var query = _context.Values.Select(row => row);
            CheckResult(query, "SELECT 'id','value' FROM 'myvalue';");
        }

        [TestMethod]
        public void SelectIntoNewObject()
        {
            var query = _context.Values.Select(r => new { Id2 = r.Id, Value2 = r.Value });
            CheckResult(query, "SELECT 'id','value' FROM 'myvalue';");
        }

        [TestMethod]
        public void WhereIdInArray()
        {
            var query = _context.Values.Where(r => new[] { 1, 2, 3, 4 }.Contains(r.Id));
            CheckResult(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id' IN (1,2,3,4);");
        }

        [TestMethod]
        public void WhereIdInList()
        {
            var query = _context.Values.Where(r => new List<int> {1,2,3,4}.Contains(r.Id));
            CheckResult(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id' IN (1,2,3,4);");
        }

        [TestMethod]
        public void WhereIdInSet()
        {
            var query = _context.Values.Where(r => new HashSet<int> { 1, 2, 3, 4 }.Contains(r.Id));
            CheckResult(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id' IN (1,2,3,4);");
        }

        [TestMethod]
        [ExpectedException(typeof(CqlLinqException),"Type System.Collections.Generic.KeyValuePair`2[System.Int32,System.String] can not be converted to a valid CQL value")]
        public void WhereKvpInDictionary()
        {
            var query = _context.Values.Where(r => new Dictionary<int, string> { { 1, "a" }, { 2, "b" }, { 3, "c" } }.Contains(new KeyValuePair<int, string>(r.Id, "a")));
            CheckResult(query, "No valid query");
        }

        [TestMethod]
        [ExpectedException(typeof(CqlLinqException), "Type System.Char can't be converted to a CQL value")]
        public void WhereIdInNotSupportedListType()
        {
            var query = _context.Values.Where(r => new List<char>{ 'a', 'b', 'c' }.Contains((char)r.Id));
            CheckResult(query, "No valid query");
        }

        [TestMethod]
        public void SelectIntoNewObjectThenWhere()
        {
            var query = _context.Values.Select(r => new { Id2 = r.Id, Value2 = r.Value }).Where(at => at.Id2==4);
            CheckResult(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id'=4;");
        }

        [TestMethod]
        public void SelectThenSelect()
        {
            var query = _context.Values.Select(r => new { Id2 = r.Id + 2, Value2 = r.Value }).Select(r2 => new { Id3 = r2.Id2 });
            CheckResult(query, "SELECT 'id' FROM 'myvalue';");
        }

        [TestMethod]
        public void OnlyWhere()
        {
            var query = _context.Values.Where(r => r.Id == 2);
            CheckResult(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id'=2;");
        }

        [TestMethod]
        [ExpectedException(typeof(CqlLinqException), "CQL does not support the Add operator")]
        public void UnParsableWhereQuery()
        {
            var query = _context.Values.Where(r => r.Id + 2 == 4);
            CheckResult(query, "no valid query");
        }

        [TestMethod]
        //[ExpectedException(typeof(CqlLinqException), "CQL does not support the Add operator")]
        public void WhereFromLinqToObjects()
        {
            var range = Enumerable.Range(1, 5);
            var selection = from r in range where r > 3 select r;
            
            var query = _context.Values.Where(r => selection.AsQueryable().Contains(r.Id));
            CheckResult(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id' IN (4,5);");
        }
    }
}

