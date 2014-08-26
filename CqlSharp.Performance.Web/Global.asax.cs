using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Http;
using System.Web.Routing;
using CqlSharp.Performance.Data;

namespace CqlSharp.Performance.Web
{
    public class WebApiApplication : System.Web.HttpApplication
    {
        protected void Application_Start()
        {
            GlobalConfiguration.Configure(WebApiConfig.Register);
            
        }

        protected void Application_End()
        {
            MeasurementManager.Disconnect();
        }
        
    }
}
