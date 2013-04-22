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
using System.Net;
using System.Threading.Tasks;

namespace CqlSharp.Protocol
{
    /// <summary>
    ///   Represents a Frame with Event data
    /// </summary>
    internal class EventFrame : Frame
    {
        /// <summary>
        ///   The name of the type of event
        /// </summary>
        public string EventType { get; private set; }

        /// <summary>
        ///   the change that occurred
        /// </summary>
        public string Change { get; set; }

        /// <summary>
        ///   the node that went up/down, or was added or deleted
        /// </summary>
        public IPEndPoint Node { get; private set; }

        /// <summary>
        ///   the altered keyspace
        /// </summary>
        public string KeySpace { get; private set; }

        /// <summary>
        ///   the altered table
        /// </summary>
        public string Table { get; private set; }

        /// <summary>
        ///   Writes the data to buffer.
        /// </summary>
        /// <param name="buffer"> The buffer. </param>
        /// <exception cref="System.NotSupportedException"></exception>
        protected override void WriteData(Stream buffer)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        ///   Initialize frame contents from the stream
        /// </summary>
        protected override async Task InitializeAsync()
        {
            EventType = await Reader.ReadStringAsync();

            if (EventType.Equals("TOPOLOGY_CHANGE", StringComparison.InvariantCultureIgnoreCase) ||
                EventType.Equals("STATUS_CHANGE", StringComparison.InvariantCultureIgnoreCase))
            {
                Change = await Reader.ReadStringAsync();
                Node = await Reader.ReadInetAsync();
            }
            else if (EventType.Equals("TOPOLOGY_CHANGE", StringComparison.InvariantCultureIgnoreCase))
            {
                Change = await Reader.ReadStringAsync();
                KeySpace = await Reader.ReadStringAsync();
                Table = await Reader.ReadStringAsync();
            }
        }
    }
}