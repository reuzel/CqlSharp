using CqlSharp.Serialization;
using System;
using System.Collections.Generic;

namespace CqlTest
{
    [CqlTable("carts")]
    public class Cart
    {
        public Guid Id { get; set; }
        public string GroupId { get; set; }
        public Dictionary<string, int> Items { get; set; }
    }
}
