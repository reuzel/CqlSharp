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

using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace CqlSharp.Linq.Expressions
{
    internal static class ReadOnlyExtensions
    {
        public static ReadOnlyCollection<TItem> AsReadOnly<TItem>(this IList<TItem> collection)
        {
            var asReadOnly = collection as ReadOnlyCollection<TItem>;
            if (asReadOnly != null)
            {
                return asReadOnly;
            }

            return collection == null
                       ? new ReadOnlyCollection<TItem>(new List<TItem>())
                       : new ReadOnlyCollection<TItem>(collection);
        }

        public static ReadOnlyDictionary<TKey, TValue> AsReadOnly<TKey, TValue>(
            this IDictionary<TKey, TValue> dictionary)
        {
            var asReadOnly = dictionary as ReadOnlyDictionary<TKey, TValue>;
            if (asReadOnly != null)
            {
                return asReadOnly;
            }

            return dictionary == null
                       ? new ReadOnlyDictionary<TKey, TValue>(new Dictionary<TKey, TValue>())
                       : new ReadOnlyDictionary<TKey, TValue>(dictionary);
        }
    }
}