using System;
using System.Diagnostics;
using CqlSharp.Protocol;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Test
{
    [TestClass]
    public class FailureTests
    {
        [TestMethod]
        public void AuthenticateError()
        {
            try
            {
                using (var connection = new CqlConnection("Servers=localhost;username=doesNotExist;password=too;loggerfactory=debug;loglevel=verbose"))
                {
                    connection.Open();
                }
            }
            catch (AuthenticationException uex)
            {
                Debug.WriteLine("Expected Unauthenticated exception: {0}", uex);
            }
            catch (Exception ex)
            {
                Assert.Fail("Wong exception thrown: {0}", ex.GetType().Name);
            }
        }
    }
}
