using System;
using System.Linq;

namespace CqlSharp.Network.Partition
{
    class ByteArrayToken : IToken
    {
        private byte[] _value;

        public void Parse(string tokenStr)
        {
            _value = System.Text.Encoding.UTF8.GetBytes(tokenStr);
        }

        public void Parse(byte[] partitionKey)
        {
            _value = partitionKey;
        }

        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;

            if (obj == null || !(obj is ByteArrayToken))
                return false;

            return _value.SequenceEqual(((ByteArrayToken)obj)._value);
        }

        public override int GetHashCode()
        {
            return _value.GetHashCode();
        }

        public int CompareTo(object obj)
        {
            var other = obj as ByteArrayToken;

            if (other == null)
                throw new ArgumentException("object not an ByteArrayToken, or null", "obj");

            for (int i = 0; i < _value.Length && i < other._value.Length; i++)
            {
                int a = (_value[i] & 0xff);
                int b = (other._value[i] & 0xff);
                if (a != b)
                    return a - b;
            }
            return 0;
        }
    }
}