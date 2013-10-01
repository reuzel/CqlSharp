using CqlSharp.Protocol;
using Microsoft.QualityTools.Testing.Fakes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Fakes;
using System.Threading.Tasks;

namespace CqlSharp.Test
{
    [TestClass]
    public class IssueTest
    {
        private const string ConnectionString =
           "server=localhost;loggerfactory=debug;loglevel=verbose";

        [TestMethod]
        public async Task Issue15()
        {
            using (ShimsContext.Create())
            {

                //Assume
                //make DateTime.Now return a value in a different timezone
                ShimDateTime.NowGet = () =>
                                          {
                                              var timezone = TimeZoneInfo.CreateCustomTimeZone("Issue15Zone",
                                                                                               TimeSpan.FromHours(-5),
                                                                                               "Issue 15 zone",
                                                                                               "Issue 15 zone");

                                              return TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, timezone);
                                          };

                //Act
                using (var connection = new CqlConnection(ConnectionString))
                {
                    await connection.OpenAsync();
                }
            }
        }
    }
}
