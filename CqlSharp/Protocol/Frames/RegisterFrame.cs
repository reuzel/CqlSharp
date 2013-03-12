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
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace CqlSharp.Protocol.Frames
{
    internal class RegisterFrame : Frame
    {
        public RegisterFrame(IList<string> eventTypes)
        {
            EventTypes = eventTypes;

            Version = FrameVersion.Request | FrameVersion.ProtocolVersion;
            Flags = FrameFlags.None;
            Stream = 0;
            OpCode = FrameOpcode.Register;
        }

        public IList<string> EventTypes { get; set; }

        protected override void WriteData(Stream buffer)
        {
            buffer.WriteStringList(EventTypes);
        }

        protected override Task InitializeAsync()
        {
            throw new NotSupportedException();
        }
    }
}