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

namespace CqlSharp.Network.nSnappy
{
    public struct HashTable
    {
        private readonly ushort[] _table;

        public HashTable(int size)
        {
            int htSize = 256;
            while(htSize < CompressorConstants.MaxHashTableSize && htSize < size)
                htSize <<= 1;

            _table = new ushort[htSize];
        }

        public uint Size
        {
            get { return (uint)_table.Length; }
        }

        public int this[uint hash]
        {
            get { return _table[hash]; }
            set { _table[hash] = (ushort)value; }
        }
    }
}