// CqlSharp - CqlSharp
// Copyright (c) 2014 Joost Reuzel
//   
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   
// http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.IO;
using System.Text;
using CqlSharp.Protocol;

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
            return new PasswordAuthenticator {Username = config.Username, Password = config.Password};
        }
    }

    /// <summary>
    /// org.apache.cassandra.auth.PasswordAuthenticator
    /// </summary>
    internal class PasswordAuthenticator : IAuthenticator
    {
        public string Username { get; set; }
        public string Password { get; set; }

        public bool Authenticate(byte protocolVersion, byte[] challenge, out byte[] response)
        {
            if(challenge == null)
            {
                if(Username == null || Password == null)
                {
                    throw new AuthenticationException(protocolVersion, "Username and Password must be provided when using PasswordAuthenticator");
                }

                var username = Encoding.UTF8.GetBytes(Username);
                var password = Encoding.UTF8.GetBytes(Password);

                using(var stream = new MemoryStream())
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

        public bool Authenticate(byte protocolVersion, byte[] finalResponse)
        {
            return true;
        }
    }
}