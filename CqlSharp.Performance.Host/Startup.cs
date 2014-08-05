using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;
using CqlSharp.Performance.Host.Annotations;
using Owin;

namespace CqlSharp.Performance.Host
{

    public class Startup
    {
        // This code configures Web API. The Startup class is specified as a type
        // parameter in the WebApp.Start method.
        [UsedImplicitly]
        public void Configuration(IAppBuilder appBuilder)
        {
            // Configure Web API for self-host. 
            var config = new HttpConfiguration();
            
            CqlSharp.Performance.Web.WebApiConfig.Register(config);

            appBuilder.UseWebApi(config);
        }
    } 

}
