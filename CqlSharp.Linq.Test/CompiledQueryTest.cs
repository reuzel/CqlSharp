using CqlSharp.Linq.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Linq.Expressions;

namespace CqlSharp.Linq.Test
{
    [TestClass]
    public class CompiledQueryTest
    {

        [TestMethod]
        public void QueryTwoArguments()
        {
            Expression<Func<MyContext, int, string, MyValue>> lambda =
                (context, id, value) => context.Values.Where(v => v.Value == value).First(val => val.Id == id);

            var provider = new CqlQueryProvider();
            var plan = (QueryPlan<MyValue>)provider.CreateQueryPlan(lambda);

            Assert.AreEqual(2, plan.VariableMap.Count);
            Assert.AreEqual(plan.VariableMap[0], 1);
            Assert.AreEqual(plan.VariableMap[1], 0);
            Assert.AreEqual("SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"value\"=? AND \"id\"=? LIMIT 1;", plan.Cql);
        }

        [TestMethod]
        public void QuerySameArgumentTwice()
        {
            Expression<Func<MyContext, int, string, MyValue>> lambda =
                (context, id, value) => context.Values.Where(v => v.Id < id).First(val => val.Id >= id);

            var provider = new CqlQueryProvider();
            var plan = (QueryPlan<MyValue>)provider.CreateQueryPlan(lambda);

            Assert.AreEqual(2, plan.VariableMap.Count);
            Assert.AreEqual(plan.VariableMap[0], 0);
            Assert.AreEqual(plan.VariableMap[1], 0);
            Assert.AreEqual("SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\"<? AND \"id\">=? LIMIT 1;", plan.Cql);
        }
    }
}
