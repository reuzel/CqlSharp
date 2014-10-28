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

using System.Numerics;

namespace CqlSharp.Linq
{
    /// <summary>
    ///   A token as returned from the CQL token() function. Instances of this class
    ///   may be (implicitly) converted to long, BigInteger or byte[] depending on 
    ///   the partitioner in use.
    /// </summary>
    public struct CqlToken
    {
        private readonly object _value;

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlToken" /> class.
        /// </summary>
        /// <param name="value"> The value. </param>
        internal CqlToken(object value)
        {
            _value = value;
        }

        /// <summary>
        ///   Gets a value indicating whether the token value is null
        /// </summary>
        /// <value> <c>true</c> if [is null]; otherwise, <c>false</c> . </value>
        public bool IsNull
        {
            get { return _value == null; }
        }

        /// <summary>
        ///   Determines whether the specified <see cref="CqlToken" />, is equal to this instance.
        /// </summary>
        /// <param name="other"> The <see cref="CqlToken" /> to compare with this instance. </param>
        /// <returns> <c>true</c> if the specified <see cref="CqlToken" /> is equal to this instance; otherwise, <c>false</c> . </returns>
        private bool Equals(CqlToken other)
        {
            return other._value.Equals(_value);
        }

        /// <summary>
        ///   Determines whether the specified <see cref="System.Object" />, is equal to this instance.
        /// </summary>
        /// <param name="obj"> The <see cref="System.Object" /> to compare with this instance. </param>
        /// <returns> <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, <c>false</c> . </returns>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is CqlToken && Equals((CqlToken) obj);
        }

        /// <summary>
        ///   Returns a hash code for this instance.
        /// </summary>
        /// <returns> A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. </returns>
        public override int GetHashCode()
        {
            return _value.GetHashCode();
        }

        public static bool operator <(CqlToken token1, CqlToken token2)
        {
            throw new CqlLinqException("The token class is intended for use in Cql linq statements only.");
        }

        public static bool operator >(CqlToken token1, CqlToken token2)
        {
            throw new CqlLinqException("The token class is intended for use in Cql linq statements only.");
        }

        public static bool operator <=(CqlToken token1, CqlToken token2)
        {
            throw new CqlLinqException("The token class is intended for use in Cql linq statements only.");
        }

        public static bool operator >=(CqlToken token1, CqlToken token2)
        {
            throw new CqlLinqException("The token class is intended for use in Cql linq statements only.");
        }

        public static bool operator ==(CqlToken token1, CqlToken token2)
        {
            throw new CqlLinqException("The token class is intended for use in Cql linq statements only.");
        }

        public static bool operator !=(CqlToken token1, CqlToken token2)
        {
            throw new CqlLinqException("The token class is intended for use in Cql linq statements only.");
        }

        public static implicit operator long(CqlToken token)
        {
            return token._value == null ? default(long) : (long) token._value;
        }

        public static implicit operator byte[](CqlToken token)
        {
            return (byte[]) token._value;
        }

        public static implicit operator BigInteger(CqlToken token)
        {
            return token._value == null ? default(BigInteger) : (BigInteger) token._value;
        }
    }
}