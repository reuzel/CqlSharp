using System;

namespace CqlSharp.Protocol
{
    /// <summary>
    /// Thrown when authentication towards the Cassandra cluster fails
    /// </summary>
    [Serializable]
    public class AuthenticationException : ProtocolException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AuthenticationException"/> class.
        /// </summary>
        /// <param name="message">The message to display for this exception.</param>
        internal AuthenticationException(string message)
            : base(Protocol.ErrorCode.BadCredentials, message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AuthenticationException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="tracingId">The tracing unique identifier.</param>
        internal AuthenticationException(string message, Guid? tracingId)
            : base(Protocol.ErrorCode.BadCredentials, message, tracingId)
        {
        }
    }
}