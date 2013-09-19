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

namespace CqlSharp.Network.Partition
{
    internal class MurmurToken : IToken
    {
        private long _value;

        #region IToken Members

        public void Parse(string tokenStr)
        {
            _value = long.Parse(tokenStr);
        }

        public void Parse(byte[] partitionKey)
        {
            long v = MurmurHash.Hash3_X64_128(partitionKey, 0, partitionKey.Length, 0)[0];
            _value = v == long.MinValue ? long.MaxValue : v;
        }

        public int CompareTo(object obj)
        {
            var other = obj as MurmurToken;

            if (other == null)
                throw new ArgumentException("object not an MurmurToken, or null", "obj");

            long otherValue = other._value;
            return _value < otherValue ? -1 : (_value == otherValue) ? 0 : 1;
        }

        #endregion

        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;
            if (obj == null || !(obj is MurmurToken))
                return false;

            return _value == ((MurmurToken) obj)._value;
        }

        public override int GetHashCode()
        {
            return (int) (_value ^ ((long) ((ulong) _value >> 32)));
        }
    }
}