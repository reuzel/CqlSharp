// CqlSharp.Linq - CqlSharp.Linq
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

namespace CqlSharp.Linq
{
    /// <summary>
    ///   Provides CQL functions that can used as part of a CQL query expression
    /// </summary>
    public static class CqlFunctions
    {
        /// <summary>
        ///   Gets the Time-To-Live of a specific identifier
        /// </summary>
        /// <param name="identifier"> The identifier. </param>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">This function can only be used as part of a Linq expression</exception>
        public static int TTL(object identifier)
        {
            throw new CqlLinqException("This function can only be used as part of a Linq expression");
        }

        /// <summary>
        ///   Gets the time a specific identifier was written to the database
        /// </summary>
        /// <param name="identifier"> The identifier. </param>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">This function can only be used as part of a Linq expression</exception>
        public static DateTime WriteTime(object identifier)
        {
            throw new CqlLinqException("This function can only be used as part of a Linq expression");
        }

        /// <summary>
        ///   Gets the (partition) token value of the given identifier
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">This function can only be used as part of a Linq expression</exception>
        public static CqlToken Token<T1>(T1 identifier)
        {
            throw new CqlLinqException("This function can only be used as part of a Linq expression");
        }

        /// <summary>
        ///   Gets the (partition) token value of the given identifier
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">This function can only be used as part of a Linq expression</exception>
        public static CqlToken Token<T1, T2>(T1 identifier, T2 identifier2)
        {
            throw new CqlLinqException("This function can only be used as part of a Linq expression");
        }

        /// <summary>
        ///   Gets the (partition) token value of the given identifier
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">This function can only be used as part of a Linq expression</exception>
        public static CqlToken Token<T1, T2, T3>(T1 identifier, T2 identifier2, T3 identifier3)
        {
            throw new CqlLinqException("This function can only be used as part of a Linq expression");
        }

        /// <summary>
        ///   Gets the (partition) token value of the given identifier
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">This function can only be used as part of a Linq expression</exception>
        public static CqlToken Token<T1, T2, T3, T4>(T1 identifier, T2 identifier2, T3 identifier3, T4 identifier4)
        {
            throw new CqlLinqException("This function can only be used as part of a Linq expression");
        }

        /// <summary>
        ///   Gets the (partition) token value of the given identifier
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">This function can only be used as part of a Linq expression</exception>
        public static CqlToken Token<T1, T2, T3, T4, T5>(T1 identifier, T2 identifier2, T3 identifier3, T4 identifier4,
                                                         T5 identifier5)
        {
            throw new CqlLinqException("This function can only be used as part of a Linq expression");
        }

        /// <summary>
        ///   Gets the (partition) token value of the given identifier
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">This function can only be used as part of a Linq expression</exception>
        public static CqlToken Token<T1, T2, T3, T4, T5, T6>(T1 identifier, T2 identifier2, T3 identifier3,
                                                             T4 identifier4, T5 identifier5, T6 identifier6)
        {
            throw new CqlLinqException("This function can only be used as part of a Linq expression");
        }

        /// <summary>
        ///   Gets the (partition) token value of the given identifier
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">This function can only be used as part of a Linq expression</exception>
        public static CqlToken Token<T1, T2, T3, T4, T5, T6, T7>(T1 identifier, T2 identifier2, T3 identifier3,
                                                                 T4 identifier4, T5 identifier5, T6 identifier6,
                                                                 T7 identifier7)
        {
            throw new CqlLinqException("This function can only be used as part of a Linq expression");
        }

        /// <summary>
        ///   Gets a TimeUuid representing the current date and time
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">This function can only be used as part of a Linq expression</exception>
        public static Guid Now()
        {
            throw new CqlLinqException("This function can only be used as part of a Linq expression");
        }

        /// <summary>
        ///   Gets a (unofficial) time UUID, that represents the minimal uuid value of a certain date time. Useful in
        ///   time comparisons based on time uuids.
        /// </summary>
        /// <param name="time"> The time. </param>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">This function can only be used as part of a Linq expression</exception>
        public static Guid MinTimeUuid(DateTime time)
        {
            throw new CqlLinqException("This function can only be used as part of a Linq expression");
        }

        /// <summary>
        ///   Gets a (unofficial) time UUID, that represents the maximum uuid value of a certain date time. Useful in
        ///   time comparisons based on time uuids.
        /// </summary>
        /// <param name="time"> The time. </param>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">This function can only be used as part of a Linq expression</exception>
        public static Guid MaxTimeUuid(DateTime time)
        {
            throw new CqlLinqException("This function can only be used as part of a Linq expression");
        }

        /// <summary>
        ///   Gets the date and time corresponding the a time uuid.
        /// </summary>
        /// <param name="timeGuid"> The time unique identifier. </param>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">This function can only be used as part of a Linq expression</exception>
        public static DateTime DateOf(Guid timeGuid)
        {
            throw new CqlLinqException("This function can only be used as part of a Linq expression");
        }

        /// <summary>
        ///   Get the unix time stamp value corresponding to the given time uuid.
        /// </summary>
        /// <param name="timeGuid"> The time unique identifier. </param>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">This function can only be used as part of a Linq expression</exception>
        public static long UnixTimeStampOf(Guid timeGuid)
        {
            throw new CqlLinqException("This function can only be used as part of a Linq expression");
        }

        //todo: blob functions	
    }
}