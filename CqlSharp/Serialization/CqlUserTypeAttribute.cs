using CqlSharp.Serialization.Marshal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Serialization
{
    /// <summary>
    /// Annotates a class indicating that it should be handled as if it is a user type
    /// </summary>
    public class CqlUserTypeAttribute : CqlCustomTypeAttribute
    {
        public CqlUserTypeAttribute() : base(typeof(UserDefinedTypeFactory))
        {
        }
    }
}
