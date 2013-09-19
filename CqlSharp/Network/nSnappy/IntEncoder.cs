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

namespace CqlSharp.Network.nSnappy
{
    public static class IntEncoder
    {
        public static byte[] Encode(int value)
        {
            const int moreData = 128;
            var uvalue = unchecked((uint) value);

            if (uvalue < 0x80)
            {
                return new[] {(byte) uvalue};
            }

            if (uvalue < 0x4000)
            {
                return new[] {(byte) (uvalue | moreData), (byte) (uvalue >> 7)};
            }

            if (uvalue < 0x200000)
            {
                return new[] {(byte) (uvalue | moreData), (byte) ((uvalue >> 7) | moreData), (byte) (uvalue >> 14)};
            }

            if (uvalue < 0x10000000)
            {
                return new[]
                           {
                               (byte) (uvalue | moreData), (byte) ((uvalue >> 7) | moreData),
                               (byte) ((uvalue >> 14) | moreData), (byte) (uvalue >> 21)
                           };
            }

            return new[]
                       {
                           (byte) (uvalue | moreData), (byte) ((uvalue >> 7) | moreData),
                           (byte) ((uvalue >> 14) | moreData), (byte) ((uvalue >> 21) | moreData), (byte) (uvalue >> 28)
                       };
        }

        public static int Decode(byte[] data, int maxEncodedBytes)
        {
            var index = 0;
            var value = 0U;

            while (index < maxEncodedBytes)
            {
                var b = data[index];
                value |= (b & 0x7fU) << index*7;

                if (b < 0x80)
                    break;

                index++;
            }

            return unchecked((int) value);
        }
    }
}