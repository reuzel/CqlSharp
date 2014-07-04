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
using System.Linq;
using System.Text;

namespace CqlSharp.Network.Partition
{
    internal class ByteArrayToken : IToken
    {
        private byte[] _value;

        #region IToken Members

        public void Parse(string tokenStr)
        {
            _value = Encoding.UTF8.GetBytes(tokenStr);
        }

        public void Parse(byte[] partitionKey)
        {
            _value = partitionKey;
        }

        public int CompareTo(object obj)
        {
            var other = obj as ByteArrayToken;

            if(other == null)
                throw new ArgumentException("object not an ByteArrayToken, or null", "obj");

            for(int i = 0; i < _value.Length && i < other._value.Length; i++)
            {
                int a = (_value[i] & 0xff);
                int b = (other._value[i] & 0xff);
                if(a != b)
                    return a - b;
            }
            return 0;
        }

        #endregion

        public override bool Equals(object obj)
        {
            if(this == obj)
                return true;

            if(obj == null || !(obj is ByteArrayToken))
                return false;

            return _value.SequenceEqual(((ByteArrayToken)obj)._value);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var result = 0;
                for(int i = 0; i < _value.Length; i++)
                {
                    byte b = _value[i];
                    result = (result*31) ^ b;
                }
                return result;
            }
        }
    }
}