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
        public void SimpleSelectQueries()
        {
            var query = _context.Values.Select(r => new { Id2 = r.Id, Value2 = r.Value });
            CheckResult(query, "SELECT 'id','value' FROM 'myvalue';");
        }

        [TestMethod]
        public void SelectThenWhereWithObjectMappingQueries()
        {
            var query = _context.Values.Select(r => new { Id2 = r.Id, Value2 = r.Value }).Where(at => at.Id2==4);
            CheckResult(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id'=4;");
        }

        [TestMethod]
        public void ConcatSelectQueries()
        {
            var query = _context.Values.Select(r => new { Id2 = r.Id + 2, Value2 = r.Value }).Select(r2 => new { Id3 = r2.Id2 });
            CheckResult(query, "SELECT 'id' FROM 'myvalue';");
        }

        [TestMethod]
        public void OnlyWhereQuery()
        {
            var query = _context.Values.Where(r => r.Id == 2);
            CheckResult(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id'=2;");
        }

        //    [TestMethod]
        //    public void UnParsableWhereQuery()
        //    {
        //        var query = _context.Values.Where(r => r.Id + 2 == 4);
        //        CheckResult(query, "select * from MyValue");
        //    }

        //    [TestMethod]
        //    public void PartiallyUnParsableWhereQuery()
        //    {
        //        var query = _context.Values.Where(r => r.Id + 2 == 4 && r.Value == "hallo");
        //        CheckResult(query, "select * from MyValue where Value=hallo");
        //    }
    }
}
