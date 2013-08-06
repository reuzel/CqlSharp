namespace CqlSharp.Network.nSnappy
{
    public struct HashTable
    {
        private readonly ushort[] _table;

        public HashTable(int size)
        {
            int htSize = 256;
            while (htSize < CompressorConstants.MaxHashTableSize && htSize < size)
            {
                htSize <<= 1;
            }

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