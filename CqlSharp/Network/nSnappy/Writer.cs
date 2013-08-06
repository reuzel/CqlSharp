using CqlSharp.Memory;

namespace CqlSharp.Network.nSnappy
{
    [System.Diagnostics.DebuggerDisplay("Value: {DebugString}")]
    class Writer
    {
        private readonly byte[] _buffer;
        private int _index;
        private readonly int _length;

        public Writer(int len)
        {
            _length = len;
            _buffer = MemoryPool.Instance.Take(len);
            _index = 0;
        }

        // ReSharper disable UnusedMember.Local
        private string DebugString
        {
            get { return System.Text.Encoding.ASCII.GetString(_buffer, 0, _index); }
        }
        // ReSharper restore UnusedMember.Local

        public bool Append(Pointer ip, int len)
        {
            int spaceLeft = _length - _index;
            if (spaceLeft < len)
            {
                return false;
            }

            var op = new Pointer(_buffer, _index);
            op.Copy(ip, len);
            _index += len;
            return true;
        }

        public bool TryFastAppend(Pointer ip, int available, int len)
        {
            int spaceLeft = _length - _index;

            if (len > 16 || available < 16 || spaceLeft < 16)
            {
                return false;
            }

            var op = new Pointer(_buffer, _index);
            op.Copy64(ip);
            op.Copy64(ip + 8, 8);

            _index += len;
            return true;
        }

        public bool AppendFromSelf(int offset, int len)
        {
            int spaceLeft = _length - _index;

            if (_index <= offset - 1u)
            {  // -1u catches offset==0
                return false;
            }

            var op = new Pointer(_buffer, _index);
            if (len <= 16 && offset >= 8 && spaceLeft >= 16)
            {
                var src = new Pointer(_buffer, _index - offset);
                op.Copy64(src);
                op.Copy64(src + 8, offset: 8);
            }
            else
            {
                if (spaceLeft >= len + CompressorConstants.MaxIncrementCopyOverflow)
                {
                    IncrementalCopyFastPath(op - offset, op, len);
                }
                else
                {
                    if (spaceLeft < len)
                    {
                        return false;
                    }

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
            } while (--len > 0);
        }

        private void IncrementalCopyFastPath(Pointer src, Pointer op, int len)
        {
            while (op - src < 8)
            {
                op.Copy64(src);
                len -= op - src;
                op += op - src;
            }

            while (len > 0)
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