using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using CqlSharp.Network;

namespace CqlSharp.Protocol
{
    /// <summary>
    /// Reader to read values from a socket. Heavily optimized to reduce task creation and byte array allocation
    /// </summary>
    internal class FrameReader : IDisposable
    {
        private const int CacheSize = 2*1024; //cache int and short values up to 2048

        private static readonly ConcurrentDictionary<byte, Task<byte>> ByteTaskCache =
            new ConcurrentDictionary<byte, Task<byte>>();

        private static readonly ConcurrentDictionary<short, Task<short>> ShortTaskCache =
            new ConcurrentDictionary<short, Task<short>>();

        private static readonly ConcurrentDictionary<int, Task<int>> IntTaskCache =
            new ConcurrentDictionary<int, Task<int>>();

        private readonly TaskCompletionSource<bool> _waitUntilAllFrameDataRead;

        private byte[] _buffer;
        private bool _disposed;
        private Stream _innerStream;
        private ArraySegment<byte> _lastReadSegment;
        private int _position;
        private int _remainingInBuffer;

        private int _unreadFromStream;

        public FrameReader(Stream innerStream, int length)
        {
            _buffer = MemoryPool.Instance.Take();

            _remainingInBuffer = 0;
            _position = 0;

            _disposed = false;

            _innerStream = innerStream;
            _unreadFromStream = length;
            _waitUntilAllFrameDataRead = new TaskCompletionSource<bool>();

            if (length == 0)
                _waitUntilAllFrameDataRead.SetResult(true);
        }

        #region Stream IO and completion

        /// <summary>
        /// Gets the wait task that completes when all frame data read from the underlying stream
        /// </summary>
        /// <value>
        /// The wait until frame data read.
        /// </value>
        public Task WaitUntilFrameDataRead
        {
            get { return _waitUntilAllFrameDataRead.Task; }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private async Task<int> ReadAsync(byte[] buffer, int offset, int count)
        {
            try
            {
                //return 0 if window size has been reached
                if (_unreadFromStream <= 0)
                    return 0;

                //get max amount of data to read
                int max = Math.Min(count, _unreadFromStream);

                //read
                int read = await _innerStream.ReadAsync(buffer, offset, max);

                //update remaining
                _unreadFromStream -= read;

                //signal EOS when window reached
                if (_unreadFromStream <= 0)
                    _waitUntilAllFrameDataRead.SetResult(true);

                //return actual read count
                return read;
            }
            catch (Exception ex)
            {
                //error no more to read
                _unreadFromStream = 0;

                //signal error as EOS
                _waitUntilAllFrameDataRead.SetException(ex);

                //rethrow
                throw;
            }
        }

        public async Task BufferRemainingData()
        {
            int newSize = _remainingInBuffer + _unreadFromStream;

            //allocate new buffer if necessary
            byte[] newBuffer = newSize > _buffer.Length ? new byte[_remainingInBuffer + _unreadFromStream] : _buffer;

            //copy/move existing buffer data
            if (_remainingInBuffer > 0)
            {
                Buffer.BlockCopy(_buffer, _position, newBuffer, 0, _remainingInBuffer);
            }

            //set read position to start of buffer
            _position = 0;

            //replace existing buffer
            if (_buffer != newBuffer)
            {
                byte[] temp = _buffer;
                _buffer = newBuffer;

                //return previous buffer to pool
                MemoryPool.Instance.Return(temp);
            }

            //load all remaining frame data
            while (_unreadFromStream > 0)
            {
                _remainingInBuffer += await ReadAsync(newBuffer, _remainingInBuffer, newSize - _remainingInBuffer);
            }
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        protected void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                _disposed = true;

                //move stream forward till complete length has been read
                try
                {
                    while (_unreadFromStream > 0)
                    {
                        _unreadFromStream -= _innerStream.Read(_buffer, 0, Math.Min(_buffer.Length, _unreadFromStream));
                    }

                    _waitUntilAllFrameDataRead.TrySetResult(true);
                }
                catch (Exception ex)
                {
                    _waitUntilAllFrameDataRead.TrySetException(ex);
                }

                _innerStream = null;

                MemoryPool.Instance.Return(_buffer);
                _buffer = null;
            }
        }

        #endregion

        #region Data Segment loading

        /// <summary>
        /// Tries to get a array segment of specific size from the buffer.
        /// </summary>
        /// <param name="size">The size.</param>
        /// <returns></returns>
        private bool TryGetSegmentFromBuffer(int size)
        {
            if (_disposed)
                throw new ObjectDisposedException("FrameReader");

            //part of current buffer
            if (size <= _remainingInBuffer)
            {
                _lastReadSegment = new ArraySegment<byte>(_buffer, _position, size);
                _position += size;
                _remainingInBuffer -= size;
                return true;
            }

            return false;
        }

        /// <summary>
        /// Reads the array segment async.
        /// </summary>
        /// <param name="size">The size.</param>
        /// <returns></returns>
        private async Task ReadSegmentAsync(int size)
        {
            if (_disposed)
                throw new ObjectDisposedException("FrameReader");

            if (_remainingInBuffer + _unreadFromStream < size)
                throw new IOException("Trying to read beyond frame length!");

            while (true)
            {
                if (TryGetSegmentFromBuffer(size))
                    return;

                //check if it would fit in the current buffer
                if (size <= _buffer.Length)
                {
                    //shift remaining buffer content to start if necessary
                    if (_position > 0)
                    {
                        Buffer.BlockCopy(_buffer, _position, _buffer, 0, _remainingInBuffer);
                        _position = 0;
                    }

                    //fill up the buffer with more data
                    int extra = await ReadAsync(_buffer, _remainingInBuffer, _buffer.Length - _remainingInBuffer);
                    if (extra == 0)
                        throw new IOException("Unexpected end of stream reached");

                    _remainingInBuffer += extra;

                    //loop to try again
                    continue;
                }

                //doesn't fit (typically a large array), allocate a new array and read the whole thing in there
                var buf = new byte[size];

                //copy already read bytes
                Buffer.BlockCopy(_buffer, _position, buf, 0, _remainingInBuffer);

                //read remaining bytes
                int read = _remainingInBuffer;
                while (read < size)
                {
                    int actual = await _innerStream.ReadAsync(buf, read, size - read);
                    if (actual == 0)
                        throw new IOException("Unexpected end of stream reached");

                    read += actual;
                }

                //set _lastReadSegment
                _lastReadSegment = new ArraySegment<byte>(buf);

                //reset read positions
                _position = 0;
                _remainingInBuffer = 0;

                //done
                return;
            }
        }

        #endregion

        /// <summary>
        /// Reads the byte async.
        /// </summary>
        /// <returns></returns>
        public Task<byte> ReadByteAsync()
        {
            if (TryGetSegmentFromBuffer(1))
            {
                byte b = _lastReadSegment.Array[_lastReadSegment.Offset];

                //return task from cache
                return ByteTaskCache.GetOrAdd(b, (val) => Task.FromResult(val));
            }

            //byte could not be obtained from cache, go the long route
            return ReadByteInternalAsync();
        }

        /// <summary>
        /// Reads the byte async, awaiting network operations
        /// </summary>
        /// <returns></returns>
        private async Task<byte> ReadByteInternalAsync()
        {
            await ReadSegmentAsync(1);
            return _lastReadSegment.Array[_lastReadSegment.Offset];
        }


        /// <summary>
        /// Reads the short async.
        /// </summary>
        /// <returns></returns>
        public Task<short> ReadShortAsync()
        {
            if (TryGetSegmentFromBuffer(2))
            {
                //reverse the value if necessary
                EnsureCorrectByteOrder();
                short value = BitConverter.ToInt16(_lastReadSegment.Array, _lastReadSegment.Offset);

                //return cached int value if possible
                if (value < CacheSize)
                    return ShortTaskCache.GetOrAdd(value, (v) => Task.FromResult(v));

                return Task.FromResult(value);
            }

            return ReadShortInternalAsync();
        }

        /// <summary>
        /// Reads the short async, awaiting network operations
        /// </summary>
        /// <returns></returns>
        private async Task<short> ReadShortInternalAsync()
        {
            await ReadSegmentAsync(2);
            EnsureCorrectByteOrder();
            short value = BitConverter.ToInt16(_lastReadSegment.Array, _lastReadSegment.Offset);
            return value;
        }

        /// <summary>
        /// Reads the int async.
        /// </summary>
        /// <returns></returns>
        public Task<int> ReadIntAsync()
        {
            if (TryGetSegmentFromBuffer(4))
            {
                EnsureCorrectByteOrder();
                int value = BitConverter.ToInt32(_lastReadSegment.Array, _lastReadSegment.Offset);

                //return cached int value if possible
                if (value < CacheSize)
                    return IntTaskCache.GetOrAdd(value, (v) => Task.FromResult(v));

                //return normal FromResult value
                return Task.FromResult(value);
            }

            return ReadIntInternalAsync();
        }

        /// <summary>
        /// Reads the int async, awaiting network operations
        /// </summary>
        /// <returns></returns>
        private async Task<int> ReadIntInternalAsync()
        {
            await ReadSegmentAsync(4);
            EnsureCorrectByteOrder();
            int value = BitConverter.ToInt32(_lastReadSegment.Array, _lastReadSegment.Offset);
            return value;
        }

        /// <summary>
        /// Reads the string async.
        /// </summary>
        /// <returns></returns>
        public async Task<string> ReadStringAsync()
        {
            //read length
            short len = await ReadShortAsync();
            if (0 == len)
            {
                return string.Empty;
            }

            //read the string segment
            if (!TryGetSegmentFromBuffer(len))
                await ReadSegmentAsync(len);

            //return parsed string
            return Encoding.UTF8.GetString(_lastReadSegment.Array, _lastReadSegment.Offset, _lastReadSegment.Count);
        }

        /// <summary>
        /// Reads the bytes async.
        /// </summary>
        /// <returns></returns>
        public async Task<byte[]> ReadBytesAsync()
        {
            int len = await ReadIntAsync();
            if (-1 == len)
            {
                return null;
            }

            //read the string segment
            if (!TryGetSegmentFromBuffer(len))
                await ReadSegmentAsync(len);

            //copy data from buffer into new array if necessary
            byte[] data = _lastReadSegment.Array == _buffer ? CopySegmentToArray() : _lastReadSegment.Array;

            return data;
        }

        /// <summary>
        /// Reads the short bytes async.
        /// </summary>
        /// <returns></returns>
        public async Task<byte[]> ReadShortBytesAsync()
        {
            short len = await ReadShortAsync();
            if (-1 == len)
            {
                return null;
            }

            //read the data segment
            if (!TryGetSegmentFromBuffer(len))
                await ReadSegmentAsync(len);

            //copy data from buffer into new array if necessary
            byte[] data = _lastReadSegment.Array == _buffer ? CopySegmentToArray() : _lastReadSegment.Array;

            return data;
        }

        /// <summary>
        /// Reads the string list async.
        /// </summary>
        /// <returns></returns>
        public async Task<IList<string>> ReadStringListAsync()
        {
            short len = await ReadShortAsync();
            var data = new string[len];
            for (int i = 0; i < len; ++i)
            {
                data[i] = await ReadStringAsync();
            }
            return data;
        }

        /// <summary>
        /// Reads the string multimap async.
        /// </summary>
        /// <returns></returns>
        public async Task<Dictionary<string, IList<string>>> ReadStringMultimapAsync()
        {
            short len = await ReadShortAsync();
            var data = new Dictionary<string, IList<string>>(len);
            for (int i = 0; i < len; ++i)
            {
                string key = await ReadStringAsync();
                IList<string> value = await ReadStringListAsync();
                data.Add(key, value);
            }

            return data;
        }

        /// <summary>
        /// Reads the inet async.
        /// </summary>
        /// <returns></returns>
        public async Task<IPEndPoint> ReadInetAsync()
        {
            byte length = await ReadByteAsync();

            if (!TryGetSegmentFromBuffer(length))
                await ReadSegmentAsync(length);

            byte[] address = CopySegmentToArray();
            var ipAddress = new IPAddress(address);

            int port = await ReadIntAsync();

            var endpoint = new IPEndPoint(ipAddress, port);

            return endpoint;
        }

        /// <summary>
        /// Reads the UUID async.
        /// </summary>
        /// <returns></returns>
        public async Task<Guid> ReadUuidAsync()
        {
            if (!TryGetSegmentFromBuffer(16))
                await ReadSegmentAsync(16);

            byte[] data = CopySegmentToArray();
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(data, 0, 4);
                Array.Reverse(data, 4, 2);
                Array.Reverse(data, 6, 2);
            }

            return new Guid(data);
        }

        /// <summary>
        /// Ensures the correct byte order for decimal conversions.
        /// </summary>
        private void EnsureCorrectByteOrder()
        {
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(_lastReadSegment.Array, _lastReadSegment.Offset, _lastReadSegment.Count);
            }
        }

        /// <summary>
        /// Copies the segment to a new array.
        /// </summary>
        /// <returns>an array with the latest data</returns>
        private byte[] CopySegmentToArray()
        {
            var arr = new byte[_lastReadSegment.Count];
            Buffer.BlockCopy(_lastReadSegment.Array, _lastReadSegment.Offset, arr, 0, _lastReadSegment.Count);
            return arr;
        }
    }
}