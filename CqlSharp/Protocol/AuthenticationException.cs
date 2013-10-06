using System;

namespace CqlSharp.Protocol
{
    [Serializable]
    public class AuthenticationException : ProtocolException
    {
        public AuthenticationException(string message)
            : base(Protocol.ErrorCode.BadCredentials, message)
        {
        }
    }
}