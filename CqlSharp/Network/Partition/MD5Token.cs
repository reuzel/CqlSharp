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
using System.Numerics;
using System.Security.Cryptography;

namespace CqlSharp.Network.Partition
{
    internal class MD5Token : IToken
    {
        private static readonly MD5 HashFunc = MD5.Create();
        private BigInteger _value;

        #region IToken Members

        public void Parse(string tokenStr)
        {
            _value = BigInteger.Parse(tokenStr);
        }

        public void Parse(byte[] partitionKey)
        {
            _value = new BigInteger(HashFunc.ComputeHash(partitionKey));
        }

        public int CompareTo(object obj)
        {
            var other = obj as MD5Token;

            if (other == null)
                throw new ArgumentException("object not an MD5Token, or null", "obj");

            return _value.CompareTo(other._value);
        }

        #endregion

        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;
            if (obj == null || !(obj is MD5Token))
                return false;

            return _value == ((MD5Token) obj)._value;
        }

        public override int GetHashCode()
        {
            return _value.GetHashCode();
        }
    }
}