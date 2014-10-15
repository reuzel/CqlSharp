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

namespace CqlSharp.Logging
{
    /// <summary>
    /// Interface towards logger implementations.
    /// </summary>
    public interface ILogger
    {
        /// <summary>
        /// Logs a message on verbose level
        /// </summary>
        /// <param name="traceId"> The trace id. </param>
        /// <param name="format"> The format. </param>
        /// <param name="values"> The values. </param>
        void LogVerbose(Guid traceId, string format, params object[] values);

        /// <summary>
        /// Logs the succesfull execution of a query
        /// </summary>
        /// <param name="traceId"> The trace id. </param>
        /// <param name="format"> The format. </param>
        /// <param name="values"> The values. </param>
        void LogQuery(Guid traceId, string format, params object[] values);

        /// <summary>
        /// Logs a message on info level
        /// </summary>
        /// <param name="traceId"> The trace id. </param>
        /// <param name="format"> The format. </param>
        /// <param name="values"> The values. </param>
        void LogInfo(Guid traceId, string format, params object[] values);

        /// <summary>
        /// Logs a message on warning level
        /// </summary>
        /// <param name="traceId"> The trace id. </param>
        /// <param name="format"> The format. </param>
        /// <param name="values"> The values. </param>
        void LogWarning(Guid traceId, string format, params object[] values);

        /// <summary>
        /// Logs a message on error level
        /// </summary>
        /// <param name="traceId"> The trace id. </param>
        /// <param name="format"> The format. </param>
        /// <param name="values"> The values. </param>
        void LogError(Guid traceId, string format, params object[] values);

        /// <summary>
        /// Logs a message on critical level
        /// </summary>
        /// <param name="traceId"> The trace id. </param>
        /// <param name="format"> The format. </param>
        /// <param name="values"> The values. </param>
        void LogCritical(Guid traceId, string format, params object[] values);
    }
}