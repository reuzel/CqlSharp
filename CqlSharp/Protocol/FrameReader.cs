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

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using CqlSharp.Memory;
using CqlSharp.Network.nSnappy;
using CqlSharp.Threading;

namespace CqlSharp.Protocol
{
    /// <summary>
    /// Reader to read values from a socket. Heavily optimized to reduce task creation and byte array allocation
    /// </summary>
    internal class FrameReader : IDisposable
    {
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
            int bufferSize = Math.Min(4*1024, length);
            _buffer = MemoryPool.Instance.Take(bufferSize);

            _remainingInBuffer = 0;
            _position = 0;

            _disposed = false;

            _innerStream = innerStream;
            _unreadFromStream = length;
            _waitUntilAllFrameDataRead = new TaskCompletionSource<bool>();

            if(length == 0)
                _waitUntilAllFrameDataRead.SetResult(true);
        }

        #region Stream IO and completion

        /// <summary>
        /// Gets the wait task that completes when all frame data read from the underlying stream
        /// </summary>
        /// <value> The wait until frame data read. </value>
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
                if(_unreadFromStream <= 0)
                    return 0;

                //get max amount of data to read
                int max = Math.Min(count, _unreadFromStream);

                //read
                int read;
                if(Scheduler.RunningSynchronously)
                    read = _innerStream.Read(buffer, offset, max);
                else
                    read = await _innerStream.ReadAsync(buffer, offset, max).AutoConfigureAwait();

                //update remaining
                _unreadFromStream -= read;

                //signal EOS when window reached
                if(_unreadFromStream <= 0)
                {
                    //Scheduler.RunOnIOThread(() => _waitUntilAllFrameDataRead.TrySetResult(true));
                    _waitUntilAllFrameDataRead.TrySetResult(true);
                }

                //return actual read count
                return read;
            }
            catch(Exception ex)
            {
                //error no more to read
                _unreadFromStream = 0;

                //signal error as EOS
                //Scheduler.RunOnIOThread(() => _waitUntilAllFrameDataRead.SetException(ex));
                _waitUntilAllFrameDataRead.SetException(ex);

                //rethrow
                throw;
            }
        }

        public async Task BufferRemainingData()
        {
            //ignore if all data has been read
            if(_unreadFromStream <= 0)
                return;

            //new buffer size
            int newSize = _remainingInBuffer + _unreadFromStream;

            //allocate new buffer if necessary
            byte[] newBuffer = newSize > _buffer.Length ? MemoryPool.Instance.Take(newSize) : _buffer;

            //copy/move existing buffer data
            if(_remainingInBuffer > 0)
                Buffer.BlockCopy(_buffer, _position, newBuffer, 0, _remainingInBuffer);

            //set read position to start of buffer
            _position = 0;

            //replace existing buffer
            if(_buffer != newBuffer)
            {
                byte[] temp = _buffer;
                _buffer = newBuffer;

                //return previous buffer to pool
                MemoryPool.Instance.Return(temp);
            }

            //load all remaining frame data
            while(_unreadFromStream > 0)
            {
                var readTask = ReadAsync(newBuffer, _remainingInBuffer, newSize - _remainingInBuffer);
                _remainingInBuffer += await readTask.AutoConfigureAwait();
            }
            }

        /// <summary>
        /// Gets a value indicating whether this frame instance content is completely read from stream.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is buffered; otherwise, <c>false</c>.
        /// </value>
        public bool IsBuffered
        {
            get { return _unreadFromStream <= 0; }
        }


        /// <summary>
        /// Decompresses the frame contents async.
        /// </summary>
        public async Task DecompressAsync()
        {
            //load all remaining frame data
            await BufferRemainingData().AutoConfigureAwait();

            //decompress into new buffer
            byte[] newBuffer;
            _remainingInBuffer = Decompressor.Decompress(_buffer, _remainingInBuffer, out newBuffer);

            //replace existing buffer
            MemoryPool.Instance.Return(_buffer);
            _buffer = newBuffer;
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing">
        /// <c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only
        /// unmanaged resources.
        /// </param>
        protected void Dispose(bool disposing)
        {
            if(!_disposed && disposing)
            {
                _disposed = true;

                //move stream forward till complete length has been read
                try
                {
                    while(_unreadFromStream > 0)
                        _unreadFromStream -= _innerStream.Read(_buffer, 0, Math.Min(_buffer.Length, _unreadFromStream));

                    //Scheduler.RunOnIOThread(() => _waitUntilAllFrameDataRead.TrySetResult(true));
                    _waitUntilAllFrameDataRead.TrySetResult(true);
                }
                catch(Exception ex)
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
        /// <param name="size"> The size. </param>
        /// <returns> </returns>
        private bool TryGetSegmentFromBuffer(int size)
        {
            if(_disposed)
                throw new ObjectDisposedException("FrameReader");

            //part of current buffer
            if(size <= _remainingInBuffer)
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
        /// <param name="size"> The size. </param>
        /// <returns> </returns>
        private async Task ReadSegmentAsync(int size)
        {
            if(_disposed)
                throw new ObjectDisposedException("FrameReader");

            if(_remainingInBuffer + _unreadFromStream < size)
                throw new IOException("Trying to read beyond frame length!");

            if(size > _buffer.Length)
            {
                //size does not fit in current buffer. Get a larger one
                var buf = MemoryPool.Instance.Take(size);

                //copy already read bytes
                Buffer.BlockCopy(_buffer, _position, buf, 0, _remainingInBuffer);

                //return old buffer, replace it with new
                MemoryPool.Instance.Return(_buffer);
                _buffer = buf;
                _position = 0;
            }

            //shift remaining buffer content to start if necessary
            if(_position + size > _buffer.Length)
            {
                Buffer.BlockCopy(_buffer, _position, _buffer, 0, _remainingInBuffer);
                _position = 0;
            }

            while(!TryGetSegmentFromBuffer(size))
            {
                //fill up the buffer with more data
                int offset = _position + _remainingInBuffer;

                //read
                int extra = await ReadAsync(_buffer, offset, _buffer.Length - offset).AutoConfigureAwait();

                if (extra == 0)
                    throw new IOException("Unexpected end of stream reached");

                _remainingInBuffer += extra;
            }
        }

        #endregion

        /// <summary>
        /// Reads the byte async.
        /// </summary>
        /// <returns> </returns>
        public Task<byte> ReadByteAsync()
        {
            if(TryGetSegmentFromBuffer(1))
            {
                byte b = _lastReadSegment.Array[_lastReadSegment.Offset];

                //return task from cache
                return b.AsTask();
            }

            //byte could not be obtained from cache, go the long route
            return ReadByteInternalAsync();
        }

        /// <summary>
        /// Reads the byte async, awaiting network operations
        /// </summary>
        /// <returns> </returns>
        private async Task<byte> ReadByteInternalAsync()
        {
            await ReadSegmentAsync(1).AutoConfigureAwait();
            return _lastReadSegment.Array[_lastReadSegment.Offset];
        }

        /// <summary>
        /// Reads the short async.
        /// </summary>
        /// <returns> </returns>
        public Task<ushort> ReadShortAsync()
        {
            if(TryGetSegmentFromBuffer(2))
            {
                //reverse the value if necessary
                ushort value = _lastReadSegment.Array.ToShort(_lastReadSegment.Offset);

                //return cached int value if possible
                return value.AsTask();
            }

            return ReadShortInternalAsync();
        }

        /// <summary>
        /// Reads the short async, awaiting network operations
        /// </summary>
        /// <returns> </returns>
        private async Task<ushort> ReadShortInternalAsync()
        {
            await ReadSegmentAsync(2).AutoConfigureAwait();

            ushort value = _lastReadSegment.Array.ToShort(_lastReadSegment.Offset);

            return value;
        }

        /// <summary>
        /// Reads the int async.
        /// </summary>
        /// <returns> </returns>
        public Task<int> ReadIntAsync()
        {
            if(TryGetSegmentFromBuffer(4))
            {
                int value = _lastReadSegment.Array.ToInt(_lastReadSegment.Offset);

                //return cached int value if possible
                return value.AsTask();
            }

            return ReadIntInternalAsync();
        }

        /// <summary>
        /// Reads the int async, awaiting network operations
        /// </summary>
        /// <returns> </returns>
        private async Task<int> ReadIntInternalAsync()
        {
            await ReadSegmentAsync(4).AutoConfigureAwait();

            int value = _lastReadSegment.Array.ToInt(_lastReadSegment.Offset);

            return value;
        }

        public Task<string> ReadStringAsync()
        {
            //read string length
            Task<ushort> lengthTask = ReadShortAsync();
            if(lengthTask.IsCompleted)
            {
                //synchronously completed (probably as it could be read from the buffer)
                ushort len = lengthTask.Result;
                
                //check for empty string
                if(0 == len)
                    return String.Empty.AsTask();

                if(TryGetSegmentFromBuffer(len))
                {
                    //yep, enough data available, return a cached version of the string
                    string str = Encoding.UTF8.GetString(_lastReadSegment.Array, _lastReadSegment.Offset, _lastReadSegment.Count);
                    return str.AsTask();
                }
            }

            //more data needs to be read, take the long way
            return ReadStringAsync(lengthTask);
        }

        /// <summary>
        /// Reads the string async.
        /// </summary>
        /// <returns> </returns>
        private async Task<string> ReadStringAsync(Task<ushort> lengthTask)
        {
            //read length
            ushort len = await lengthTask.AutoConfigureAwait();
            if (0 == len)
                return string.Empty;

            //read the string segment
            if (!TryGetSegmentFromBuffer(len))
                await ReadSegmentAsync(len).AutoConfigureAwait();

            //return parsed string
            return Encoding.UTF8.GetString(_lastReadSegment.Array, _lastReadSegment.Offset, _lastReadSegment.Count);
        }

        /// <summary>
        /// Reads the bytes async.
        /// </summary>
        /// <returns> </returns>
        public async Task<byte[]> ReadBytesAsync()
        {
            int len = await ReadIntAsync().AutoConfigureAwait();
            if (-1 == len)
                return null;

            //read the string segment
            if (!TryGetSegmentFromBuffer(len))
                await ReadSegmentAsync(len).AutoConfigureAwait();

            //copy data from buffer into new array if necessary
            byte[] data = _lastReadSegment.Array == _buffer ? CopySegmentToArray() : _lastReadSegment.Array;

            return data;
        }

        /// <summary>
        /// Reads the short bytes async.
        /// </summary>
        /// <returns> </returns>
        public async Task<byte[]> ReadShortBytesAsync()
        {
            ushort len = await ReadShortAsync().AutoConfigureAwait();

            if(len == 0)
                return new byte[0];

            //read the data segment
            if (!TryGetSegmentFromBuffer(len))
                await ReadSegmentAsync(len).AutoConfigureAwait();

            //copy data from buffer into new array if necessary
            byte[] data = _lastReadSegment.Array == _buffer ? CopySegmentToArray() : _lastReadSegment.Array;

            return data;
        }

        /// <summary>
        /// Reads the string list async.
        /// </summary>
        /// <returns> </returns>
        public async Task<IList<string>> ReadStringListAsync()
        {
            ushort len = await ReadShortAsync().AutoConfigureAwait();
            var data = new string[len];
            for(int i = 0; i < len; ++i)
            {
                data[i] = await ReadStringAsync().AutoConfigureAwait(); 
            }
            return data;
        }

        /// <summary>
        /// Reads the string multimap async.
        /// </summary>
        /// <returns> </returns>
        public async Task<Dictionary<string, IList<string>>> ReadStringMultimapAsync()
        {
            ushort len = await ReadShortAsync().AutoConfigureAwait();
            var data = new Dictionary<string, IList<string>>(len);
            for(int i = 0; i < len; ++i)
            {
                string key = await ReadStringAsync().AutoConfigureAwait();
                IList<string> value = await ReadStringListAsync().AutoConfigureAwait();
                data.Add(key, value);
            }

            return data;
        }

        /// <summary>
        /// Reads the inet async.
        /// </summary>
        /// <returns> </returns>
        public async Task<IPEndPoint> ReadInetAsync()
        {
            byte length = await ReadByteAsync().AutoConfigureAwait();

            if(!TryGetSegmentFromBuffer(length))
                await ReadSegmentAsync(length).AutoConfigureAwait();

            byte[] address = CopySegmentToArray();
            var ipAddress = new IPAddress(address);

            int port = await ReadIntAsync().AutoConfigureAwait();

            var endpoint = new IPEndPoint(ipAddress, port);

            return endpoint;
        }

        /// <summary>
        /// Reads the UUID async.
        /// </summary>
        /// <returns> </returns>
        public async Task<Guid> ReadUuidAsync()
        {
            if (!TryGetSegmentFromBuffer(16))
                await ReadSegmentAsync(16).AutoConfigureAwait();

            return _lastReadSegment.Array.ToGuid(_lastReadSegment.Offset);
        }

        /// <summary>
        /// Copies the segment to a new array.
        /// </summary>
        /// <returns> an array with the latest data </returns>
        private byte[] CopySegmentToArray()
        {
            var arr = new byte[_lastReadSegment.Count];
            Buffer.BlockCopy(_lastReadSegment.Array, _lastReadSegment.Offset, arr, 0, _lastReadSegment.Count);
            return arr;
        }
    }
}