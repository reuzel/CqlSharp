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
using System.Collections.Generic;

namespace CqlSharp.Network
{
    internal static class EnumerableExtensions
    {
        /// <summary>
        ///   Findes the smallest value in an enumeration. If the enumeration is empty, the default value is returned.
        /// </summary>
        /// <remarks>
        ///   Functionally identical to source.OrderBy(comparisonValueFunc).FirstOrDefault()
        /// </remarks>
        /// <typeparam name="T"> type of the enumeration </typeparam>
        /// <param name="source"> The source enumeration. </param>
        /// <param name="comparisonValueFunc"> The comparison value func. </param>
        /// <returns> The smallest item in the enumeration, based on the comparison value function, or default(T) if the enumeration does not contain any items. </returns>
        public static T SmallestOrDefault<T>(this IEnumerable<T> source, Func<T, int> comparisonValueFunc)
        {
            //iterate explicitly to check if the source is empty
            IEnumerator<T> enumerator = source.GetEnumerator();
            if (!enumerator.MoveNext())
                return default(T);

            T selected = enumerator.Current;
            int compValue = comparisonValueFunc(selected);

            while (enumerator.MoveNext())
            {
                T newItem = enumerator.Current;
                int newValue = comparisonValueFunc(newItem);
                if (newValue < compValue)
                {
                    selected = newItem;
                    compValue = newValue;
                }
            }

            return selected;
        }
    }
}