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

namespace CqlSharp.Network.Partition
{
    /// <summary>
    ///   Token as used to route a query to a node based on partition key column values.
    /// </summary>
    internal interface IToken : IComparable
    {
        /// <summary>
        ///   Parses the specified token STR.
        /// </summary>
        /// <param name="tokenStr"> The token STR. </param>
        void Parse(string tokenStr);

        /// <summary>
        ///   Parses the specified partition key.
        /// </summary>
        /// <param name="partitionKey"> The partition key. </param>
        void Parse(byte[] partitionKey);
    }
}