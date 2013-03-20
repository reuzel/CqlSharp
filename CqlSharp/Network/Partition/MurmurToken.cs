using System;

namespace CqlSharp.Network.Partition
{
    class MurmurToken : IToken
    {
        private long _value;

        public void Parse(string tokenStr)
        {
            _value = long.Parse(tokenStr);
        }

        public void Parse(byte[] partitionKey)
        {
            long v = (long)MurmurHash.Hash3_x64_128(partitionKey, 0, partitionKey.Length, 0)[0];
            _value = v == long.MinValue ? long.MaxValue : v;
        }

        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;
            if (obj == null || !(obj is MurmurToken))
                return false;

            return _value == ((MurmurToken)obj)._value;
        }

        public override int GetHashCode()
        {
            return (int)(_value ^ ((long)((ulong)_value >> 32)));
        }

        public int CompareTo(object obj)
        {
            var other = obj as MurmurToken;

            if (other == null)
                throw new ArgumentException("object not an MurmurToken, or null", "obj");

            long otherValue = other._value;
            return _value < otherValue ? -1 : (_value == otherValue) ? 0 : 1;
        }
    }
}