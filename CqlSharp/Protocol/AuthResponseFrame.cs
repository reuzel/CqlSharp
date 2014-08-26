// CqlSharp - CqlSharp
// Copyright (c) 2013 Joost Reuzel
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

using System;
using System.IO;
using System.Threading.Tasks;
using CqlSharp.Threading;

namespace CqlSharp.Protocol
{
    internal class AuthResponseFrame : Frame
    {
        /// <summary>
        ///   Initializes a new instance of the <see cref="AuthResponseFrame" /> class.
        /// </summary>
        /// <param name="saslResponse"> The sasl response. </param>
        public AuthResponseFrame(byte[] saslResponse)
        {
            Version = FrameVersion.Request;
            Flags = FrameFlags.None;
            Stream = 0;
            OpCode = FrameOpcode.AuthResponse;

            SaslResponse = saslResponse;
        }

        public byte[] SaslResponse { get; set; }

        protected override void WriteData(Stream buffer)
        {
            buffer.WriteByteArray(SaslResponse);
        }


        protected override Task InitializeAsync()
        {
            throw new NotSupportedException();
        }
    }
}