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

namespace CqlSharp.Protocol.Exceptions
{
    [Serializable]
    public class UnavailableException : ProtocolException
    {
        public UnavailableException(string message, CqlConsistency cqlConsistency, int required, int alive)
            : base(ErrorCode.Unavailable, message)
        {
            CqlConsistency = cqlConsistency;
            Required = required;
            Alive = alive;
        }

        public CqlConsistency CqlConsistency { get; private set; }

        public int Required { get; private set; }

        public int Alive { get; private set; }
    }
}