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
    /// Converts Types by copying values based on column names
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class CqlEntityConverter<T> : ITypeConverter<T>
    {
        /// <summary>
        /// Converts the source object to an object of the the given target type.
        /// </summary>
        /// <typeparam name="TTarget">The type of the target.</typeparam>
        /// <param name="source">The source.</param>
        /// <returns>an object of the the given target type</returns>
        public TTarget ConvertTo<TTarget>(T source)
        {
            return CopyValues<T, TTarget>(source);
        }

        /// <summary>
        /// Converts an object of the given source type to an instance of this converters type
        /// </summary>
        /// <typeparam name="TSource">The type of the source.</typeparam>
        /// <param name="source">The source.</param>
        /// <returns></returns>
        public T ConvertFrom<TSource>(TSource source)
        {
            return CopyValues<TSource, T>(source);
        }

        /// <summary>
        /// Copies the values from two types based CqlColumn annotations.
        /// </summary>
        /// <typeparam name="TSource">The type of the source.</typeparam>
        /// <typeparam name="TTarget">The type of the target.</typeparam>
        /// <param name="source">The source.</param>
        /// <returns></returns>
        private static TTarget CopyValues<TSource, TTarget>(TSource source)
        {
            var sourceAccessor = ObjectAccessor<TSource>.Instance;
            var targetAccessor = ObjectAccessor<TTarget>.Instance;

            var result = Activator.CreateInstance<TTarget>();

            foreach(var column in sourceAccessor.Columns)
            {
                ICqlColumnInfo<TTarget> targetColumn;
                if(targetAccessor.ColumnsByName.TryGetValue(column.Name, out targetColumn))
                    column.CopyValue(source, result, targetColumn);
            }

            return result;
        }
    }
}