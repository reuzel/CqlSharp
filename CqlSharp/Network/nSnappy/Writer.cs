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

using System.Diagnostics;
using System.Text;
using CqlSharp.Memory;

namespace CqlSharp.Network.nSnappy
{
    [DebuggerDisplay("Value: {DebugString}")]
    internal class Writer
    {
        private readonly byte[] _buffer;
        private readonly int _length;
        private int _index;

        public Writer(int len)
        {
            _length = len;
            _buffer = MemoryPool.Instance.Take(len);
            _index = 0;
        }

        // ReSharper disable UnusedMember.Local
        private string DebugString
        {
            get { return Encoding.ASCII.GetString(_buffer, 0, _index); }
        }

        // ReSharper restore UnusedMember.Local

        public bool Append(Pointer ip, int len)
        {
            int spaceLeft = _length - _index;
            if(spaceLeft < len)
                return false;

            var op = new Pointer(_buffer, _index);
            op.Copy(ip, len);
            _index += len;
            return true;
        }

        public bool TryFastAppend(Pointer ip, int available, int len)
        {
            int spaceLeft = _length - _index;

            if(len > 16 || available < 16 || spaceLeft < 16)
                return false;

            var op = new Pointer(_buffer, _index);
            op.Copy64(ip);
            op.Copy64(ip + 8, 8);

            _index += len;
            return true;
        }

        public bool AppendFromSelf(int offset, int len)
        {
            int spaceLeft = _length - _index;

            if(_index <= offset - 1u)
            {
                // -1u catches offset==0
                return false;
            }

            var op = new Pointer(_buffer, _index);
            if(len <= 16 && offset >= 8 && spaceLeft >= 16)
            {
                var src = new Pointer(_buffer, _index - offset);
                op.Copy64(src);
                op.Copy64(src + 8, 8);
            }
            else
            {
                if(spaceLeft >= len + CompressorConstants.MaxIncrementCopyOverflow)
                    IncrementalCopyFastPath(op - offset, op, len);
                else
                {
                    if(spaceLeft < len)
                        return false;

                    IncrementalCopy(op - offset, op, len);
                }
            }

            _index += len;
            return true;
        }

        private void IncrementalCopy(Pointer src, Pointer op, int len)
        {
            do
            {
                op[0] = src[0];

                op += 1;
                src += 1;
            } while(--len > 0);
        }

        private void IncrementalCopyFastPath(Pointer src, Pointer op, int len)
        {
            while(op - src < 8)
            {
                op.Copy64(src);
                len -= op - src;
                op += op - src;
            }

            while(len > 0)
            {
                op.Copy64(src);
                src += 8;
                op += 8;
                len -= 8;
            }
        }

        public byte[] ToArray()
        {
            return _buffer;
        }
    }
}