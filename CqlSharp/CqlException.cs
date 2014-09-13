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
using System.Data.Common;
using System.Runtime.Serialization;

namespace CqlSharp
{
    /// <summary>
    /// Exception representint Cql specific errors
    /// </summary>
    [Serializable]
    public class CqlException : DbException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CqlException" /> class.
        /// </summary>
        /// <param name="message"> The error message that explains the reason for the exception. </param>
        /// <param name="inner">
        /// The exception that is the cause of the current exception. If the <paramref name="inner" />
        /// parameter is not null, the current exception is raised in a catch block that handles the inner exception.
        /// </param>
        internal CqlException(string message, Exception inner)
            : base(message, inner)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlException" /> class.
        /// </summary>
        /// <param name="message"> The message to display for this exception. </param>
        internal CqlException(string message)
            : base(message)
        {
        }
    }
}