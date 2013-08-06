﻿namespace CqlSharp.Network.nSnappy
{
    public struct VarInt32
    {
        private const int MaxEncodedBytes = 5;

        public int Value { get; private set; }

        public VarInt32(int value)
            : this()
        {
            Value = value;
        }

        public VarInt32(byte[] data)
            : this()
        {
            Value = IntEncoder.Decode(data, MaxEncodedBytes);
        }

        public byte[] GetEncodedValue()
        {
            return IntEncoder.Encode(Value);
        }
    }
}