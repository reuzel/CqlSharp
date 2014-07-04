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
    public static class CompressorConstants
    {
        public const int MaxIncrementCopyOverflow = 10;
        public const int BlockLog = 15;
        public const int BlockSize = 1 << BlockLog;

        public const int MaxHashTableBits = 14;
        public const int MaxHashTableSize = 1 << MaxHashTableBits;
    }
}