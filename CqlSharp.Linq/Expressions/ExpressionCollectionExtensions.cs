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

using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace CqlSharp.Linq.Expressions
{
    internal static class ExpressionCollectionExtensions
    {
        public static ReadOnlyCollection<TItem> AsReadOnly<TItem>(this IEnumerable<TItem> collection)
        {
            var asReadOnly = collection as ReadOnlyCollection<TItem>;
            if (asReadOnly != null)
            {
                return asReadOnly;
            }

            var list = collection == null
                           ? new List<TItem>()
                           : collection as IList<TItem> ?? new List<TItem>(collection);

            return new ReadOnlyCollection<TItem>(list);
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

        /// <summary>
        ///   Visits the expression collection.
        /// </summary>
        /// <param name="visitor"> The visitor. </param>
        /// <param name="expressions"> The expressions. </param>
        /// <param name="changed"> true if the collection is changed, false if visits did not return in any change </param>
        /// <returns> a collection of visited expressions, or the original collection if it did not change </returns>
        public static IEnumerable<T> VisitAll<T>(this IEnumerable<T> expressions, ExpressionVisitor visitor,
                                                 out bool changed) where T : Expression
        {
            if (expressions == null)
            {
                changed = false;
                return null;
            }

            changed = false;
            var visited = new List<T>();
            foreach (T expr in expressions)
            {
                var v = (T) visitor.Visit(expr);
                visited.Add(v);
                changed |= v != expr;
            }

            return changed ? visited : expressions;
        }

        /// <summary>
        ///   Visits the expression collection.
        /// </summary>
        /// <param name="visitor"> The visitor. </param>
        /// <param name="expressions"> The expressions. </param>
        /// <param name="changed"> true if the collection is changed, false if visits did not return in any change </param>
        /// <returns> a collection of visited expressions, or the original collection if it did not change </returns>
        public static IDictionary<TKey, TValue> VisitAll<TKey, TValue>(this IDictionary<TKey, TValue> expressions,
                                                                       ExpressionVisitor visitor, out bool changed)
            where TKey : Expression
            where TValue : Expression
        {
            if (expressions == null)
            {
                changed = false;
                return null;
            }

            changed = false;
            var visited = new Dictionary<TKey, TValue>();
            foreach (var kvp in expressions)
            {
                var vKey = (TKey) visitor.Visit(kvp.Key);
                var vValue = (TValue) visitor.Visit(kvp.Value);

                visited.Add(vKey, vValue);
                changed = changed || (vKey != kvp.Key) || (vValue != kvp.Value);
            }

            return changed ? visited : expressions;
        }
    }
}