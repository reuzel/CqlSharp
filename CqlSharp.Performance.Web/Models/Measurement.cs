using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using CqlSharp.Serialization;

namespace CqlSharp.Performance.Web.Models
{
    /// <summary>
    /// Single measurement for a customer
    /// </summary>
    public class Measurement
    {
        [CqlKey(IsPartitionKey = true, Order = 0)]
        public int Id { get; set; }

        [CqlKey(IsPartitionKey = true, Order = 1)]
        public string Customer { get; set; }

        public Dictionary<string, int> Values { get; set; }
    }
}