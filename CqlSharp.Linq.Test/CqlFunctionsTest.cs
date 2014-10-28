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
using System.Linq;
using System.Linq.Expressions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Linq.Test
{
    [TestClass]
    public class CqlFunctionsTest
    {
        [TestMethod]
        public void UnixTimestampOfNow()
        {
            Expression<Func<MyContext, object>> query =
                context => context.Values.Select(v => CqlFunctions.UnixTimeStampOf(CqlFunctions.Now())).ToList();

            LinqTest.CheckCql(query, "SELECT unixtimestampof(now()) FROM \"myvalue\";");
        }

        [TestMethod]
        public void TokenComparison()
        {
            Expression<Func<MyContext, object>> query =
                context => context.Values.Where(v => CqlFunctions.Token(v.Id) < CqlFunctions.Token(0)).ToList();
            
            LinqTest.CheckCql(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE token(\"id\")<token(0);");
        }

        [TestMethod]
        public void SelectTokenAndCompare()
        {
            Expression<Func<MyContext, object>> query =
                context =>
                context.Values.Select(v => CqlFunctions.Token(v.Id)).Where(tid => tid < CqlFunctions.Token(0)).ToList();
            
            LinqTest.CheckCql(query, "SELECT token(\"id\") FROM \"myvalue\" WHERE token(\"id\")<token(0);");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException))]
        public void CompareWithoutATerm()
        {
            Expression<Func<MyContext, object>> query =
                context =>
                context.Values.Select(v => new {v.Id, Token = CqlFunctions.Token(v.Id)}).Where(
                    tid => tid.Token < CqlFunctions.Token(tid.Id)).ToList();
            
            LinqTest.CheckCql(query, "illegal");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException))]
        public void TTLInWhereClause()
        {
            Expression<Func<MyContext, object>> query =
                context => context.Values.Where(v => CqlFunctions.TTL(v.Value) < 600).ToList();
            
            LinqTest.CheckCql(query, "illegal");
        }

        [TestMethod]
        public void WhereThenSelectAllowFiltering()
        {
            Expression<Func<MyContext, object>> query =
                context =>
                context.Values.Where(p => p.Value == "hallo daar").Select(r => r.Id).AllowFiltering().ToList();

            LinqTest.CheckCql(query, "SELECT \"id\" FROM \"myvalue\" WHERE \"value\"='hallo daar' ALLOW FILTERING;");
        }
    }
}