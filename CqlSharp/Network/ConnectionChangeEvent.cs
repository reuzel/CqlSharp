using System;

namespace CqlSharp.Network
{
    public class ConnectionChangeEvent : EventArgs
    {
        public bool Connected { get; set; }

        public bool Failure
        {
            get { return Exception != null; }
        }

        public Exception Exception { get; set; }
    }
}