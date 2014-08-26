using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqlSharp.Serialization;

namespace CqlSharp.Performance.Data
{
    [CqlTable("measurements")]
    public class Measurement
    {
        public int Id { get; set; }
        public string Customer { get; set; }

        public Dictionary<string, int> Values { get; set; }
    }
}
