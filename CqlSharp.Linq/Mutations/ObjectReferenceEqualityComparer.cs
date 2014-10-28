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
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    ///   A generic object comparerer that would only use object's reference, 
    ///   ignoring any <see cref="IEquatable{T}" /> or <see cref="object.Equals(object)" />  overrides.
    /// </summary>
    internal class ObjectReferenceEqualityComparer<T> : IEqualityComparer<T>, IEqualityComparer where T : class
    {
        public static readonly IEqualityComparer<T> Instance = new ObjectReferenceEqualityComparer<T>();

        #region IEqualityComparer Members

        /// <summary>
        ///   Determines whether the specified objects are equal.
        /// </summary>
        /// <returns> true if the specified objects are equal; otherwise, false. </returns>
        /// <param name="x"> The first object to compare. </param>
        /// <param name="y"> The second object to compare. </param>
        /// <exception cref="T:System.ArgumentException">
        ///   <paramref name="x" />
        ///   and
        ///   <paramref name="y" />
        ///   are of different types and neither one can handle comparisons with the other.</exception>
        public new bool Equals(object x, object y)
        {
            return ReferenceEquals(x, y);
        }

        /// <summary>
        ///   Returns a hash code for the specified object.
        /// </summary>
        /// <returns> A hash code for the specified object. </returns>
        /// <param name="obj"> The <see cref="T:System.Object" /> for which a hash code is to be returned. </param>
        /// <exception cref="T:System.ArgumentNullException">The type of
        ///   <paramref name="obj" />
        ///   is a reference type and
        ///   <paramref name="obj" />
        ///   is null.</exception>
        public int GetHashCode(object obj)
        {
            return RuntimeHelpers.GetHashCode(obj);
        }

        #endregion

        #region IEqualityComparer<T> Members

        /// <summary>
        ///   Determines whether the specified objects are equal.
        /// </summary>
        /// <param name="x"> The first object of type <paramref name="T" /> to compare. </param>
        /// <param name="y"> The second object of type <paramref name="T" /> to compare. </param>
        /// <returns> true if the specified objects are equal; otherwise, false. </returns>
        public bool Equals(T x, T y)
        {
            return ReferenceEquals(x, y);
        }

        /// <summary>
        ///   Returns a hash code for this instance.
        /// </summary>
        /// <param name="obj"> The object. </param>
        /// <returns> A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. </returns>
        public int GetHashCode(T obj)
        {
            return RuntimeHelpers.GetHashCode(obj);
        }

        #endregion
    }
}