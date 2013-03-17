using System;
using System.Numerics;
using System.Security.Cryptography;

namespace CqlSharp.Network.Partition
{
    class MD5Token : IToken
    {
        private BigInteger _value;
        private static readonly MD5 HashFunc = MD5.Create();

        public void Parse(string tokenStr)
        {
            _value = BigInteger.Parse(tokenStr);
        }

        public void Parse(byte[] partitionKey)
        {
            _value = new BigInteger(HashFunc.ComputeHash(partitionKey));
        }

        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;
            if (obj == null || !(obj is MD5Token))
                return false;

            return _value == ((MD5Token)obj)._value;
        }

        public override int GetHashCode()
        {
            return _value.GetHashCode();
        }

        public int CompareTo(object obj)
        {
            var other = obj as MD5Token;

            if (other == null)
                throw new ArgumentException("object not an MD5Token, or null", "obj");

            return _value.CompareTo(other._value);
        }
    }
}