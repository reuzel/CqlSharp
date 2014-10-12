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

namespace CqlSharp.Authentication
{
    /// <summary>
    /// Implemented by Authentication algorithms
    /// </summary>
    public interface IAuthenticator
    {
        /// <summary>
        /// Authenticates using the specified Sasl challenge.
        /// </summary>
        /// <param name="protocolVersion">The version of the protocol in use</param>
        /// <param name="challenge"> The challenge. </param>
        /// <param name="response"> The response. </param>
        /// <returns> true, if authentication may continue </returns>
        bool Authenticate(byte protocolVersion, byte[] challenge, out byte[] response);

        /// <summary>
        /// Authenticates the specified final SASL response.
        /// </summary>
        /// <param name="protocolVersion">The version of the protocol in use</param>
        /// <param name="finalResponse"> The final response. </param>
        /// <returns> true, if authentication is succesful </returns>
        bool Authenticate(byte protocolVersion, byte[] finalResponse);
    }
}