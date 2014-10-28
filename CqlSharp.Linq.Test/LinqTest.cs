// CqlSharp.Linq - CqlSharp.Linq.Test
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
using System.Linq;
using System.Linq.Expressions;
using CqlSharp.Linq.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Linq.Test
{
    [TestClass]
    public class LinqTest
    {
        public static void CheckCql(Expression query, string expectedCql)
        {
            var provider = new CqlQueryProvider();
            var plan = provider.CreateQueryPlan(query);
            Assert.AreEqual(expectedCql, plan.Cql);
        }

        [TestMethod]
        public void WhereThenSelect()
        {
            var filter = "hallo";

            Expression<Func<MyContext, object>> query =
                context => context.Values.Where(p => p.Value == filter + " daar").Select(r => r.Id).ToList();

            CheckCql(query, "SELECT \"id\" FROM \"myvalue\" WHERE \"value\"='hallo daar';");
        }

        [TestMethod]
        public void SelectThenWhere()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.Select(r => r.Id).Where(id => id == 4).ToList();

            CheckCql(query, "SELECT \"id\" FROM \"myvalue\" WHERE \"id\"=4;");
        }

        [TestMethod]
        public void NoWhereOrSelect()
        {
            Expression<Func<MyContext, object>> query = context => context.GetTable<AnnotatedTable>().ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"linqtest\".\"myvalue\";");
        }

        [TestMethod]
        public void SelectAll()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.Select(row => row).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\";");
        }

        [TestMethod]
        public void SelectIntoNewObject()
        {
            Expression<Func<MyContext, object>> query =
                context => context.Values.Select(r => new {Id2 = r.Id, Value2 = r.Value}).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\";");
        }

        [TestMethod]
        public void WhereIdInArray()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.Where(r => new[] {1, 2, 3, 4}.Contains(r.Id)).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\" IN (1,2,3,4);");
        }

        [TestMethod]
        public void WhereIdInList()
        {
            Expression<Func<MyContext, object>> query =
                context => context.Values.Where(r => new List<int> {1, 2, 3, 4}.Contains(r.Id)).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\" IN (1,2,3,4);");
        }

        [TestMethod]
        public void WhereIdInSet()
        {
            Expression<Func<MyContext, object>> query =
                context => context.Values.Where(r => new HashSet<int> {1, 2, 3, 4}.Contains(r.Id)).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\" IN (1,2,3,4);");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlException))]
        public void WhereKvpInDictionary()
        {
            Expression<Func<MyContext, object>> query =
                context =>
                context.Values.Where(
                    r =>
                    new Dictionary<int, string> {{1, "a"}, {2, "b"}, {3, "c"}}.Contains(
                        new KeyValuePair<int, string>(r.Id, "a"))).ToList();
            CheckCql(query, "No valid query");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlException))]
        public void WhereIdInNotSupportedListType()
        {
            Expression<Func<MyContext, object>> query =
                context => context.Values.Where(r => new List<char> {'a', 'b', 'c'}.Contains((char) r.Id)).ToList();
            CheckCql(query, "No valid query");
        }

        [TestMethod]
        public void SelectIntoNewObjectThenWhere()
        {
            Expression<Func<MyContext, object>> query =
                context =>
                context.Values.Select(r => new {Id2 = r.Id, Value2 = r.Value}).Where(at => at.Id2 == 4).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\"=4;");
        }

        [TestMethod]
        public void SelectThenSelect()
        {
            Expression<Func<MyContext, object>> query =
                context =>
                context.Values.Select(r => new {Id2 = r.Id + 2, Value2 = r.Value}).Select(r2 => new {Id3 = r2.Id2}).
                    ToList();
            CheckCql(query, "SELECT \"id\" FROM \"myvalue\";");
        }

        [TestMethod]
        public void OnlyWhere()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.Where(r => r.Id == 2).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\"=2;");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException), "CQL does not support the Add operator")]
        public void UnParsableWhereQuery()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.Where(r => r.Id + 2 == 4).ToList();
            CheckCql(query, "no valid query");
        }

        [TestMethod]
        //[ExpectedException(typeof(CqlLinqException), "CQL does not support the Add operator")]
        public void WhereFromLinqToObjects()
        {
            var range = Enumerable.Range(1, 5);
            var selection = from r in range where r > 3 select r;

            Expression<Func<MyContext, object>> query = context => context.Values.Where(r => selection.Contains(r.Id)).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\" IN (4,5);");
        }

        [TestMethod]
        public void OnlyFirst()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.First();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" LIMIT 1;");
        }

        [TestMethod]
        public void FirstWithPredicate()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.First(v => v.Id == 2);
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\"=2 LIMIT 1;");
        }

        [TestMethod]
        public void SelectThenFirst()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.Select(v => new {Id2 = v.Id}).First();
            CheckCql(query, "SELECT \"id\" FROM \"myvalue\" LIMIT 1;");
        }

        [TestMethod]
        public void SelectThenWhereThenFirst()
        {
            Expression<Func<MyContext, object>> query =
                context => context.Values.Select(v => new {Id2 = v.Id}).Where(v2 => v2.Id2 == 2).First();
            CheckCql(query, "SELECT \"id\" FROM \"myvalue\" WHERE \"id\"=2 LIMIT 1;");
        }

        [TestMethod]
        public void SelectThenFirstWithPredicate()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.Select(v => new {Id2 = v.Id}).First(v2 => v2.Id2 == 2);
            CheckCql(query, "SELECT \"id\" FROM \"myvalue\" WHERE \"id\"=2 LIMIT 1;");
        }

        [TestMethod]
        public void OnlyFirstOrDefault()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.FirstOrDefault();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" LIMIT 1;");
        }

        [TestMethod]
        public void FirstOrDefaultWithPredicate()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.FirstOrDefault(v => v.Id == 2);
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\"=2 LIMIT 1;");
        }

        [TestMethod]
        public void CountWithPredicate()
        {
            Expression<Func<MyContext, int>> query = context => context.Values.Count(v => v.Id == 2);
            CheckCql(query, "SELECT COUNT(*) FROM \"myvalue\" WHERE \"id\"=2;");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException))]
        public void TakeBeforeWhere()
        {
            //Wrong: logically first three items of values  table are taken, then where is performed on those three values, but Cql does not support sub-queries so this will not provide expected results
            Expression<Func<MyContext, object>> query = context => context.Values.Take(3).Where(v => v.Id == 2).ToList();
            CheckCql(query, "invalid query");
        }

        [TestMethod]
        public void WhereThenTake()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.Where(v => v.Id == 2).Take(3).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\"=2 LIMIT 3;");
        }

        [TestMethod]
        public void LargeTakeThenSmallTake()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.Take(3).Take(1).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" LIMIT 1;");
        }

        [TestMethod]
        public void SmallTakeThenLargeTake()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.Take(1).Take(3).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" LIMIT 1;");
        }

        [TestMethod]
        public void TakeThenCount()
        {
            Expression<Func<MyContext, int>> query = context => context.Values.Take(100).Count();
            CheckCql(query, "SELECT COUNT(*) FROM \"myvalue\" LIMIT 100;");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException))]
        public void TakeThenCountWithCondition()
        {
            Expression<Func<MyContext, int>> query = context => context.Values.Take(100).Count(v => v.Id > 100);
            CheckCql(query, "invalid query");
        }

        [TestMethod]
        public void SelectIntoNewObjectThenWhereThenTake()
        {
            Expression<Func<MyContext, object>> query =
                context =>
                context.Values.Select(r => new {Id2 = r.Id, Value2 = r.Value}).Where(at => at.Id2 == 4).Take(3).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\"=4 LIMIT 3;");
        }

        [TestMethod]
        public void OrderBy()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.OrderBy(v => v.Id).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" ORDER BY \"id\" ASC;");
        }

        [TestMethod]
        public void SelectIntoNewObjectThenOrderBy()
        {
            Expression<Func<MyContext, object>> query =
                context => context.Values.Select(r => new {Id2 = r.Id, Value2 = r.Value}).OrderBy(at => at.Id2).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" ORDER BY \"id\" ASC;");
        }

        [TestMethod]
        public void OrderByThenByDescending()
        {
            Expression<Func<MyContext, object>> query =
                context => context.Values.OrderBy(v => v.Id).ThenByDescending(v2 => v2.Value).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" ORDER BY \"id\" ASC,\"value\" DESC;");
        }

        [TestMethod]
        public void OrderByThenOrderBy()
        {
            Expression<Func<MyContext, object>> query =
                context => context.Values.OrderBy(v => v.Id).OrderByDescending(v2 => v2.Value).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" ORDER BY \"id\" ASC,\"value\" DESC;");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException))]
        public void TakeBeforeOrderBy()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.Take(4).OrderBy(v => v.Id).ToList();
            CheckCql(query, "invalid");
        }

        [TestMethod]
        public void OrderByThenTake()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.OrderBy(v => v.Id).Take(4).ToList();
            CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" ORDER BY \"id\" ASC LIMIT 4;");
        }

        [TestMethod]
        public void SelectDistinct()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.Select(v => v.Id).Distinct().ToList();
            CheckCql(query, "SELECT DISTINCT \"id\" FROM \"myvalue\";");
        }

        [TestMethod]
        public void SelectDistinctTake()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.Select(v => v.Id).Distinct().Take(3).ToList();
            CheckCql(query, "SELECT DISTINCT \"id\" FROM \"myvalue\" LIMIT 3;");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException))]
        public void SelectTakeThenDistinct()
        {
            Expression<Func<MyContext, object>> query = context => context.Values.Select(v => v.Id).Take(3).Distinct().ToList();
            CheckCql(query, "SELECT DISTINCT \"id\" FROM \"myvalue\" LIMIT 3;");
        }
    }
}