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
using System.Net;

namespace CqlSharp.Network
{
    /// <summary>
    ///   Event raised when a cluster's topology changes.
    /// </summary>
    internal class ClusterChangedEvent : EventArgs
    {
        public ClusterChangedEvent(ClusterChange change, IPAddress node)
        {
            Node = node;
            Change = change;
        }

        public ClusterChange Change { get; private set; }
        public IPAddress Node { get; private set; }
    }
}