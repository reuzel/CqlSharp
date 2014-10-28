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

using System.Linq.Expressions;
using CqlSharp.Linq.Query;
using CqlSharp.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Linq.Test
{
    /// <summary>
    ///   The context used for testing
    /// </summary>
    public class MyContext : CqlContext
    {
        public MyContext()
        {
        }

        public MyContext(string connectionString)
            : base(connectionString)
        {
        }

        public MyContext(CqlConnection connection, bool ownsConnection)
            : base(connection, ownsConnection)
        {
        }

        public CqlTable<MyValue> Values { get; set; }
    }


    /// <summary>
    ///   class representing the values in a table
    /// </summary>
    public class MyValue
    {
        [CqlKey]
        public int Id { get; set; }

        public string Value { get; set; }
    }

    /// <summary>
    ///   Similar to MyValue, but now annotated with key and keyspace data
    /// </summary>
    [CqlTable("myvalue", Keyspace = "linqtest")]
    public class AnnotatedTable
    {
        [CqlKey]
        [CqlColumn("id")]
        public int Id { get; set; }

        [CqlColumn("value", CqlTypeCode.Ascii)]
        public string Value { get; set; }
    }
}