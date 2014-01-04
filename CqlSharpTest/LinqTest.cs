using CqlSharp.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace CqlSharp.Test
{
    [TestClass]
    public class LinqTest
    {
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

        private delegate object QueryFunc(MyContext context);

        private void ExecuteQuery(QueryFunc query, string cql)
        {
            using (var queryWriter = new StringWriter())
            using (var context = new MyContext { SkipExecute = true, Log = queryWriter })
            {
                var result = query(context);
                Assert.AreEqual(cql, queryWriter.ToString().TrimEnd());
            }
        }

        [TestMethod]
        public void WhereThenSelect()
        {
            var filter = "hallo";

            QueryFunc query = (context) => context.Values.Where(p => p.Value == filter + " daar").Select(r => r.Id).ToList();

            ExecuteQuery(query, "SELECT 'id' FROM 'myvalue' WHERE 'value'='hallo daar';");
        }

        [TestMethod]
        public void SelectThenWhere()
        {
            QueryFunc query = context => context.Values.Select(r => r.Id).Where(id => id == 4).ToList();

            ExecuteQuery(query, "SELECT 'id' FROM 'myvalue' WHERE 'id'=4;");
        }

        [TestMethod]
        public void NoWhereOrSelect()
        {
            QueryFunc query = context => context.Values.ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue';");
        }

        [TestMethod]
        public void SelectAll()
        {
            QueryFunc query = context => context.Values.Select(row => row).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue';");
        }

        [TestMethod]
        public void SelectIntoNewObject()
        {
            QueryFunc query = context => context.Values.Select(r => new { Id2 = r.Id, Value2 = r.Value }).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue';");
        }

        [TestMethod]
        public void WhereIdInArray()
        {
            QueryFunc query = context => context.Values.Where(r => new[] { 1, 2, 3, 4 }.Contains(r.Id)).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id' IN (1,2,3,4);");
        }

        [TestMethod]
        public void WhereIdInList()
        {
            QueryFunc query = context => context.Values.Where(r => new List<int> { 1, 2, 3, 4 }.Contains(r.Id)).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id' IN (1,2,3,4);");
        }

        [TestMethod]
        public void WhereIdInSet()
        {
            QueryFunc query = context => context.Values.Where(r => new HashSet<int> { 1, 2, 3, 4 }.Contains(r.Id)).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id' IN (1,2,3,4);");
        }

        [TestMethod]
        [ExpectedException(typeof(CqlLinqException), "Type System.Collections.Generic.KeyValuePair`2[System.Int32,System.String] can not be converted to a valid CQL value")]
        public void WhereKvpInDictionary()
        {
            QueryFunc query = context => context.Values.Where(r => new Dictionary<int, string> { { 1, "a" }, { 2, "b" }, { 3, "c" } }.Contains(new KeyValuePair<int, string>(r.Id, "a"))).ToList();
            ExecuteQuery(query, "No valid query");
        }

        [TestMethod]
        [ExpectedException(typeof(CqlLinqException), "Type System.Char can't be converted to a CQL value")]
        public void WhereIdInNotSupportedListType()
        {
            QueryFunc query = context => context.Values.Where(r => new List<char> { 'a', 'b', 'c' }.Contains((char)r.Id)).ToList();
            ExecuteQuery(query, "No valid query");
        }

        [TestMethod]
        public void SelectIntoNewObjectThenWhere()
        {
            QueryFunc query = context => context.Values.Select(r => new { Id2 = r.Id, Value2 = r.Value }).Where(at => at.Id2 == 4).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id'=4;");
        }

        [TestMethod]
        public void SelectThenSelect()
        {
            QueryFunc query = context => context.Values.Select(r => new { Id2 = r.Id + 2, Value2 = r.Value }).Select(r2 => new { Id3 = r2.Id2 }).ToList();
            ExecuteQuery(query, "SELECT 'id' FROM 'myvalue';");
        }

        [TestMethod]
        public void OnlyWhere()
        {
            QueryFunc query = context => context.Values.Where(r => r.Id == 2).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id'=2;");
        }

        [TestMethod]
        [ExpectedException(typeof(CqlLinqException), "CQL does not support the Add operator")]
        public void UnParsableWhereQuery()
        {
            QueryFunc query = context => context.Values.Where(r => r.Id + 2 == 4).ToList();
            ExecuteQuery(query, "no valid query");
        }

        [TestMethod]
        //[ExpectedException(typeof(CqlLinqException), "CQL does not support the Add operator")]
        public void WhereFromLinqToObjects()
        {
            var range = Enumerable.Range(1, 5);
            var selection = from r in range where r > 3 select r;

            QueryFunc query = context => context.Values.Where(r => selection.AsQueryable().Contains(r.Id)).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id' IN (4,5);");
        }

        [TestMethod]
        public void OnlyFirst()
        {
            QueryFunc query = context => context.Values.First();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' LIMIT 1;");
        }

        [TestMethod]
        public void FirstWithPredicate()
        {
            QueryFunc query = context => context.Values.First(v => v.Id == 2);
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id'=2 LIMIT 1;");
        }

        [TestMethod]
        public void SelectThenFirst()
        {
            QueryFunc query = context => context.Values.Select(v => new { Id2 = v.Id }).First();
            ExecuteQuery(query, "SELECT 'id' FROM 'myvalue' LIMIT 1;");
        }

        [TestMethod]
        public void SelectThenWhereThenFirst()
        {
            QueryFunc query = context => context.Values.Select(v => new { Id2 = v.Id }).Where(v2 => v2.Id2 == 2).First();
            ExecuteQuery(query, "SELECT 'id' FROM 'myvalue' WHERE 'id'=2 LIMIT 1;");
        }

        [TestMethod]
        public void SelectThenFirstWithPredicate()
        {
            QueryFunc query = context => context.Values.Select(v => new { Id2 = v.Id }).First(v2 => v2.Id2 == 2);
            ExecuteQuery(query, "SELECT 'id' FROM 'myvalue' WHERE 'id'=2 LIMIT 1;");
        }

        [TestMethod]
        public void OnlyFirstOrDefault()
        {
            QueryFunc query = context => context.Values.FirstOrDefault();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' LIMIT 1;");
        }

        [TestMethod]
        public void FirstOrDefaultWithPredicate()
        {
            QueryFunc query = context => context.Values.FirstOrDefault(v => v.Id == 2);
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id'=2 LIMIT 1;");
        }

        [TestMethod]
        public void CountWithPredicate()
        {
            QueryFunc query = context => context.Values.Count(v => v.Id == 2);
            ExecuteQuery(query, "SELECT COUNT(*) FROM 'myvalue' WHERE 'id'=2;");
        }

        [TestMethod]
        [ExpectedException(typeof(CqlLinqException))]
        public void TakeBeforeWhere()
        {
            //Wrong: logically first three items of values  table are taken, then where is performed on those three values, but Cql does not support sub-queries so this will not provide expected results
            QueryFunc query = context => context.Values.Take(3).Where(v => v.Id == 2).ToList();
            ExecuteQuery(query, "invalid query");
        }

        [TestMethod]
        public void WhereThenTake()
        {
            QueryFunc query = context => context.Values.Where(v => v.Id == 2).Take(3).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id'=2 LIMIT 3;");
        }

        [TestMethod]
        public void LargeTakeThenSmallTake()
        {
            QueryFunc query = context => context.Values.Take(3).Take(1).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' LIMIT 1;");
        }

        [TestMethod]
        public void SmallTakeThenLargeTake()
        {
            QueryFunc query = context => context.Values.Take(1).Take(3).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' LIMIT 1;");
        }

        [TestMethod]
        public void SelectIntoNewObjectThenWhereThenTake()
        {
            QueryFunc query = context => context.Values.Select(r => new { Id2 = r.Id, Value2 = r.Value }).Where(at => at.Id2 == 4).Take(3).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id'=4 LIMIT 3;");
        }
        
    }
}

