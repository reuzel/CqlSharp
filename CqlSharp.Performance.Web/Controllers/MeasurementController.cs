using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Web.Http;
using CqlSharp.Performance.Web.Models;

namespace CqlSharp.Performance.Web.Controllers
{
    public class MeasurementController : ApiController
    {
        private static readonly List<Measurement> Measurements = new List<Measurement>
        {
            new Measurement { Customer = "Joost", Id = 1, Values = new Dictionary<string, int> {{"temp", 27}, {"humidity", 70}, {"pressure", 5}}},
            new Measurement { Customer = "Joost", Id = 2, Values = new Dictionary<string, int> {{"temp", 28}, {"humidity", 65}, {"pressure", 4}}},
            new Measurement { Customer = "Joost", Id = 3, Values = new Dictionary<string, int> {{"temp", 29}, {"humidity", 70}, {"pressure", 8}}},
            new Measurement { Customer = "Joost", Id = 4, Values = new Dictionary<string, int> {{"temp", 17}, {"humidity", 71}, {"pressure", 12}}},
            new Measurement { Customer = "Joost", Id = 5, Values = new Dictionary<string, int> {{"temp", 18}, {"humidity", 69}, {"pressure", 2}}},
        };

        public List<Measurement> GetAllMeasurements()
        {
            return Measurements;
        }

        public Measurement GetMeasurementByCustomerAndId(string customer, int id)
        {
            return Measurements.FirstOrDefault(m => m.Customer.Equals(customer) && m.Id == id);
        }

    }
}
