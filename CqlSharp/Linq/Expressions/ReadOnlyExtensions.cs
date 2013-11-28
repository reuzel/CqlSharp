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

            return collection == null ? new ReadOnlyCollection<TItem>(new List<TItem>()) : new ReadOnlyCollection<TItem>(collection);
        }

        public static ReadOnlyDictionary<TKey, TValue> AsReadOnly<TKey, TValue>(this IDictionary<TKey, TValue> dictionary)
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
