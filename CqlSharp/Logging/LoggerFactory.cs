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

namespace CqlSharp.Logging
{
    internal static class LoggerFactory
    {
        /// <summary>
        /// Creates a logger instance with the specified name.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns></returns>
        public static Logger Create(string name)
        {
            return new Logger(name);
        }
    }
}