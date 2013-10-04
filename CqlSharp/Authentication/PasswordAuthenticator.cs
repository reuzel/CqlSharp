using System.IO;
using System.Text;

namespace CqlSharp.Authentication
{
    /// <summary>
    /// Factory for username/password authentication. Matches org.apache.cassandra.auth.PasswordAuthenticator
    /// </summary>
    internal class PasswordAuthenticatorFactory : IAuthenticatorFactory
    {
        public string Name
        {
            get { return "org.apache.cassandra.auth.PasswordAuthenticator"; }
        }

        public IAuthenticator CreateAuthenticator(CqlConnectionStringBuilder config)
        {
            return new PasswordAuthenticator { Username = config.Username, Password = config.Password };
        }
    }
    /// <summary>
    /// org.apache.cassandra.auth.PasswordAuthenticator
    /// </summary>
    internal class PasswordAuthenticator : IAuthenticator
    {
        public string Username { get; set; }
        public string Password { get; set; }

        public bool Authenticate(byte[] challenge, out byte[] response)
        {
            if (challenge == null)
            {
                var username = Encoding.UTF8.GetBytes(Username);
                var password = Encoding.UTF8.GetBytes(Password);

                using (var stream = new MemoryStream())
                {
                    stream.WriteByte(0);
                    stream.Write(username, 0, username.Length);
                    stream.WriteByte(0);
                    stream.Write(password, 0, password.Length);

                    response = stream.ToArray();
                    return true;
                }
            }

            response = null;
            return false;
        }

        public bool Authenticate(byte[] finalResponse)
        {
            return true;
        }
    }
}
