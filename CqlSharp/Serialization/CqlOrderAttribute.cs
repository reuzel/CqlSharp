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

using System;

namespace CqlSharp.Serialization
{
    /// <summary>
    ///   Indicates the order in which this column is to be placed
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class CqlOrderAttribute : Attribute
    {
        private readonly int _order;

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlOrderAttribute"/> class.
        /// </summary>
        /// <param name="order">The order.</param>
        public CqlOrderAttribute(int order)
        {
            _order = order;
        }

        /// <summary>
        ///   Gets the name of the index.
        /// </summary>
        /// <value> The name. </value>
        public int Order
        {
            get { return _order; }
        }
    }
}