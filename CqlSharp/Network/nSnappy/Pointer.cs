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

namespace CqlSharp.Network.nSnappy
{
    public struct Pointer : IEquatable<Pointer>
    {
        private readonly byte[] _buffer;
        private readonly string _name;
        private readonly int _position;

        public Pointer(Pointer copyFrom)
        {
            _buffer = copyFrom._buffer;
            _position = copyFrom._position;
            _name = null;
        }

        public Pointer(byte[] buffer, int position = 0, string name = null)
        {
            _buffer = buffer;
            _position = position;
            _name = name;
        }

        public byte[] Array
        {
            get { return _buffer; }
        }

        public byte this[int offset]
        {
            get { return _buffer[_position + offset]; }
            set { _buffer[_position + offset] = value; }
        }

        #region IEquatable<Pointer> Members

        public bool Equals(Pointer other)
        {
            return Equals(other._buffer, _buffer) && other._position == _position;
        }

        #endregion

        public static implicit operator int(Pointer pointer)
        {
            return pointer._position;
        }

        public static Pointer operator +(Pointer pointer, int value)
        {
            return new Pointer(pointer._buffer, pointer._position + value, pointer._name);
        }

        public static Pointer operator +(Pointer pointer, uint value)
        {
            return new Pointer(pointer._buffer, (int) (pointer._position + value), pointer._name);
        }

        public static Pointer operator -(Pointer pointer, int value)
        {
            return new Pointer(pointer._buffer, pointer._position - value, pointer._name);
        }

        public void Copy(Pointer source, int length)
        {
            Buffer.BlockCopy(source._buffer, source._position, _buffer, _position, length);
        }

        public void Copy64(Pointer source, int offset = 0)
        {
            offset += _position;
            for (var i = 0; i < 8; i++)
            {
                _buffer[offset + i] = source[i];
            }
        }

        public void WriteUInt16(int value)
        {
            _buffer[_position] = (byte) (value & 0xff);
            _buffer[_position + 1] = (byte) (value >> 8 & 0xff);
        }

        public uint ToUInt32(int offset = 0)
        {
            var l = _buffer.Length;

            uint value = this[offset];
            value |= (_position + offset + 1 >= l ? 0u : this[offset + 1]) << 8;
            value |= (_position + offset + 2 >= l ? 0u : this[offset + 2]) << 16;
            value |= (_position + offset + 3 >= l ? 0u : this[offset + 3]) << 24;

            return value;
        }

        public override string ToString()
        {
            var name = _name ?? "<???>";
            return _position == 0
                       ? string.Format("{0}[{1}]", name, _buffer.Length)
                       : string.Format("{0}[{1}]+{2}", name, _buffer.Length, _position);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (Pointer)) return false;
            return Equals((Pointer) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (_buffer.GetHashCode()*397) ^ _position;
            }
        }

        public static bool operator ==(Pointer left, Pointer right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Pointer left, Pointer right)
        {
            return !Equals(left, right);
        }
    }
}