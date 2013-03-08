// CqlSharp
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

namespace CqlSharp.Protocol
{
    /// <summary>
    /// DateTime extensions to convert date-time values to and from unix-time
    /// </summary>
    internal static class DateTimeExtensions
    {
        public static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Translates the DateTime to a unix/POSIX timestamp
        /// </summary>
        /// <param name="datetime">The datetime.</param>
        /// <returns></returns>
        public static long ToTimestamp(this DateTime datetime)
        {
            return (long)datetime.ToUniversalTime().Subtract(Epoch).TotalMilliseconds;
        }

        /// <summary>
        /// Translates a unix/POSIX timestamp to a DateTime
        /// </summary>
        /// <param name="timestamp">The timestamp.</param>
        /// <returns></returns>
        public static DateTime ToDateTime(this long timestamp)
        {
            return Epoch.AddMilliseconds(timestamp);
        }
    }
}