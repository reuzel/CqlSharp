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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using CqlSharp.Threading;

namespace CqlSharp.Memory
{
    /// <summary>
    /// MemoryStream that uses memory from the MemoryPool
    /// </summary>
    internal class PoolMemoryStream : Stream
    {
        private const int BufferSize = 8192;
        private int _bufferCount;
        private byte[][] _buffers;

        //private readonly List<byte[]> _buffers;
        private bool _disposed;
        private long _position;
        private long _size;

        /// <summary>
        /// Initializes a new instance of the <see cref="PoolMemoryStream" /> class.
        /// </summary>
        public PoolMemoryStream()
        {
            _buffers = new byte[10][];
            _bufferCount = 0;
            _size = 0;
            _position = 0;
            _disposed = false;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PoolMemoryStream" /> class.
        /// </summary>
        /// <param name="data"> The data. </param>
        public PoolMemoryStream(byte[] data)
        {
            _buffers = new byte[10][];
            _bufferCount = 0;

            _size = 0;
            _position = 0;
            _disposed = false;

            WriteInternal(data, 0, data.Length);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PoolMemoryStream" /> class.
        /// </summary>
        /// <param name="size"> The size. </param>
        public PoolMemoryStream(long size)
        {
            _buffers = new byte[10][];
            _bufferCount = 0;

            _size = 0;
            _position = 0;
            _disposed = false;

            SetLengthInternal(size);
        }

        /// <summary>
        /// Gets the capacity of the current stream. Capacity will grow or reduce when data is read or written.
        /// </summary>
        /// <value> The capacity. </value>
        public long Capacity
        {
            get { return _buffers.Length*BufferSize; }
        }

        /// <summary>
        /// When overridden in a derived class, gets a value indicating whether the current stream supports reading.
        /// </summary>
        /// <returns> true if the stream supports reading; otherwise, false. </returns>
        /// <filterpriority>1</filterpriority>
        public override bool CanRead
        {
            get
            {
                if(_disposed) throw new ObjectDisposedException("PoolMemoryStream");
                return true;
            }
        }

        /// <summary>
        /// When overridden in a derived class, gets a value indicating whether the current stream supports seeking.
        /// </summary>
        /// <returns> true if the stream supports seeking; otherwise, false. </returns>
        /// <filterpriority>1</filterpriority>
        public override bool CanSeek
        {
            get
            {
                if(_disposed) throw new ObjectDisposedException("PoolMemoryStream");
                return true;
            }
        }

        /// <summary>
        /// When overridden in a derived class, gets a value indicating whether the current stream supports writing.
        /// </summary>
        /// <returns> true if the stream supports writing; otherwise, false. </returns>
        /// <filterpriority>1</filterpriority>
        public override bool CanWrite
        {
            get
            {
                if(_disposed) throw new ObjectDisposedException("PoolMemoryStream");
                return true;
            }
        }

        /// <summary>
        /// When overridden in a derived class, gets the length in bytes of the stream.
        /// </summary>
        /// <returns> A long value representing the length of the stream in bytes. </returns>
        /// <exception cref="T:System.NotSupportedException">A class derived from Stream does not support seeking.</exception>
        /// <exception cref="T:System.ObjectDisposedException">Methods were called after the stream was closed.</exception>
        /// <filterpriority>1</filterpriority>
        public override long Length
        {
            get
            {
                if(_disposed) throw new ObjectDisposedException("PoolMemoryStream");
                return _size;
            }
        }

        /// <summary>
        /// When overridden in a derived class, gets or sets the position within the current stream.
        /// </summary>
        /// <returns> The current position within the stream. </returns>
        /// <exception cref="T:System.IO.IOException">An I/O error occurs.</exception>
        /// <exception cref="T:System.NotSupportedException">The stream does not support seeking.</exception>
        /// <exception cref="T:System.ObjectDisposedException">Methods were called after the stream was closed.</exception>
        /// <filterpriority>1</filterpriority>
        public override long Position
        {
            get
            {
                if(_disposed) throw new ObjectDisposedException("PoolMemoryStream");

                return _position;
            }
            set
            {
                if(_disposed) throw new ObjectDisposedException("PoolMemoryStream");

                if(value < 0 || value >= _size)
                {
                    throw new ArgumentOutOfRangeException("value",
                                                          "Position should be smaller than current stream size, and larger then or equal to 0");
                }


                _position = value;
            }
        }

        public override bool CanTimeout
        {
            get { return false; }
        }

        /// <summary>
        /// Gets or sets the <see cref="System.Byte" /> at the specified index.
        /// </summary>
        /// <value> The <see cref="System.Byte" /> . </value>
        /// <param name="index"> The index. </param>
        /// <returns> </returns>
        public byte this[long index]
        {
            get
            {
                //get location in buffer
                var bufferIndex = (int)((index)/BufferSize);
                var bufferOffset = (int)((index)%BufferSize);

                //copy value to internal buffers
                return _buffers[bufferIndex][bufferOffset];
            }

            set
            {
                //get location in buffer
                var bufferIndex = (int)((index)/BufferSize);
                var bufferOffset = (int)((index)%BufferSize);

                //copy value to internal buffers
                _buffers[bufferIndex][bufferOffset] = value;
            }
        }

        /// <summary>
        /// Adds a buffer.
        /// </summary>
        private void AddBuffer()
        {
            if(++_bufferCount > _buffers.Length)
                Array.Resize(ref _buffers, _buffers.Length + 10);

            _buffers[_bufferCount - 1] = MemoryPool.Instance.Take(BufferSize);
        }

        /// <summary>
        /// When overridden in a derived class, clears all buffers for this stream and causes any buffered data to be written to
        /// the underlying device.
        /// </summary>
        /// <exception cref="T:System.IO.IOException">An I/O error occurs.</exception>
        /// <filterpriority>2</filterpriority>
        public override void Flush()
        {
            if(_disposed) throw new ObjectDisposedException("PoolMemoryStream");
        }

        /// <summary>
        /// Asynchronously clears all buffers for this stream, causes any buffered data to be written to the underlying device, and
        /// monitors cancellation requests.
        /// </summary>
        /// <param name="cancellationToken">
        /// The token to monitor for cancellation requests. The default value is
        /// <see
        ///     cref="P:System.Threading.CancellationToken.None" />
        /// .
        /// </param>
        /// <returns> A task that represents the asynchronous flush operation. </returns>
        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            if(_disposed) throw new ObjectDisposedException("PoolMemoryStream");

            return TaskCache.CompletedTask;
        }

        /// <summary>
        /// When overridden in a derived class, sets the position within the current stream.
        /// </summary>
        /// <returns> The new position within the current stream. </returns>
        /// <param name="offset"> A byte offset relative to the <paramref name="origin" /> parameter. </param>
        /// <param name="origin">
        /// A value of type <see cref="T:System.IO.SeekOrigin" /> indicating the reference point used to
        /// obtain the new position.
        /// </param>
        /// <exception cref="T:System.IO.IOException">An I/O error occurs.</exception>
        /// <exception cref="T:System.NotSupportedException">
        /// The stream does not support seeking, such as if the stream is
        /// constructed from a pipe or console output.
        /// </exception>
        /// <exception cref="T:System.ObjectDisposedException">Methods were called after the stream was closed.</exception>
        /// <filterpriority>1</filterpriority>
        public override long Seek(long offset, SeekOrigin origin)
        {
            if(_disposed) throw new ObjectDisposedException("PoolMemoryStream");

            switch(origin)
            {
                case SeekOrigin.Begin:
                    if(offset >= _size)
                        throw new ArgumentOutOfRangeException("offset", "Offset is larger than stream size");

                    _position = offset;
                    break;
                case SeekOrigin.End:
                    if(offset >= _size)
                        throw new ArgumentOutOfRangeException("offset", "Offset is larger than stream size");

                    _position = _size - offset;
                    break;
                case SeekOrigin.Current:
                    long newPosition = _position + offset;

                    if(newPosition >= _size || newPosition < 0)
                    {
                        throw new ArgumentOutOfRangeException("offset",
                                                              "Offset moves position before start or after end of stream");
                    }

                    _position = newPosition;
                    break;
            }

            return _position;
        }

        /// <summary>
        /// When overridden in a derived class, sets the length of the current stream.
        /// </summary>
        /// <param name="value"> The desired length of the current stream in bytes. </param>
        /// <exception cref="T:System.IO.IOException">An I/O error occurs.</exception>
        /// <exception cref="T:System.NotSupportedException">
        /// The stream does not support both writing and seeking, such as if the
        /// stream is constructed from a pipe or console output.
        /// </exception>
        /// <exception cref="T:System.ObjectDisposedException">Methods were called after the stream was closed.</exception>
        /// <filterpriority>2</filterpriority>
        public override void SetLength(long value)
        {
            SetLengthInternal(value);
        }

        /// <summary>
        /// sets the length of the current stream.
        /// </summary>
        /// <param name="value"> The desired length of the current stream in bytes. </param>
        /// <exception cref="T:System.IO.IOException">An I/O error occurs.</exception>
        /// <exception cref="T:System.NotSupportedException">
        /// The stream does not support both writing and seeking, such as if the
        /// stream is constructed from a pipe or console output.
        /// </exception>
        /// <exception cref="T:System.ObjectDisposedException">Methods were called after the stream was closed.</exception>
        /// <filterpriority>2</filterpriority>
        private void SetLengthInternal(long value)
        {
            if(_disposed) throw new ObjectDisposedException("PoolMemoryStream");

            _size = value;

            int bufferIndex = (int)_size/BufferSize;

            //clear redundant buffers
            int count = _bufferCount;
            for(int i = bufferIndex + 1; i < count; i++)
            {
                MemoryPool.Instance.Return(_buffers[i]);
                _buffers[i] = null;
                _bufferCount--;
            }

            //add new buffers
            for(int j = _bufferCount; j <= bufferIndex; j++)
            {
                AddBuffer();
            }
        }

        /// <summary>
        /// Asynchronously reads a sequence of bytes from the current stream, advances the position within the stream by the number
        /// of bytes read, and monitors cancellation requests.
        /// </summary>
        /// <param name="buffer"> The buffer to write the data into. </param>
        /// <param name="offset"> The byte offset in <paramref name="buffer" /> at which to begin writing data from the stream. </param>
        /// <param name="count"> The maximum number of bytes to read. </param>
        /// <param name="cancellationToken">
        /// The token to monitor for cancellation requests. The default value is
        /// <see
        ///     cref="P:System.Threading.CancellationToken.None" />
        /// .
        /// </param>
        /// <returns>
        /// A task that represents the asynchronous read operation. The value of the
        /// <paramref>
        ///     <name>int</name>
        /// </paramref>
        /// parameter contains the total number of bytes read into the buffer. The result value can be less than the number of
        /// bytes requested if the number of bytes currently available is less than the requested number, or it can be 0 (zero) if
        /// the end of the stream has been reached.
        /// </returns>
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            int read = Read(buffer, offset, count);
            return read.AsTask();
        }

        /// <summary>
        /// When overridden in a derived class, reads a sequence of bytes from the current stream and advances the position within
        /// the stream by the number of bytes read.
        /// </summary>
        /// <returns>
        /// The total number of bytes read into the buffer. This can be less than the number of bytes requested if that
        /// many bytes are not currently available, or zero (0) if the end of the stream has been reached.
        /// </returns>
        /// <param name="buffer">
        /// An array of bytes. When this method returns, the buffer contains the specified byte array with the values between
        /// <paramref
        ///     name="offset" />
        /// and ( <paramref name="offset" /> + <paramref name="count" /> - 1) replaced by the bytes read from the current source.
        /// </param>
        /// <param name="offset">
        /// The zero-based byte offset in <paramref name="buffer" /> at which to begin storing the data read
        /// from the current stream.
        /// </param>
        /// <param name="count"> The maximum number of bytes to be read from the current stream. </param>
        /// <exception cref="T:System.ArgumentException">
        /// The sum of
        /// <paramref name="offset" />
        /// and
        /// <paramref name="count" />
        /// is larger than the buffer length.
        /// </exception>
        /// <exception cref="T:System.ArgumentNullException">
        /// <paramref name="buffer" />
        /// is null.
        /// </exception>
        /// <exception cref="T:System.ArgumentOutOfRangeException">
        /// <paramref name="offset" />
        /// or
        /// <paramref name="count" />
        /// is negative.
        /// </exception>
        /// <exception cref="T:System.IO.IOException">An I/O error occurs.</exception>
        /// <exception cref="T:System.NotSupportedException">The stream does not support reading.</exception>
        /// <exception cref="T:System.ObjectDisposedException">Methods were called after the stream was closed.</exception>
        /// <filterpriority>1</filterpriority>
        public override int Read(byte[] buffer, int offset, int count)
        {
            if(_disposed) throw new ObjectDisposedException("PoolMemoryStream");

            //get location in buffer
            var bufferIndex = (int)(_position/BufferSize);
            var bufferOffset = (int)(_position%BufferSize);

            var maxReadable = (int)Math.Min(count, _size - _position);
            var toRead = maxReadable;
            while(toRead > 0)
            {
                int copySize = Math.Min(toRead, BufferSize - bufferOffset);
                Buffer.BlockCopy(_buffers[bufferIndex], bufferOffset, buffer, offset, copySize);
                bufferIndex++;
                bufferOffset = 0;
                offset += copySize;
                toRead -= copySize;
            }

            _position += maxReadable;
            return maxReadable;
        }

        /// <summary>
        /// Reads a byte from the stream and advances the position within the stream by one byte, or returns -1 if at the end of
        /// the stream.
        /// </summary>
        /// <returns> The unsigned byte cast to an Int32, or -1 if at the end of the stream. </returns>
        public override int ReadByte()
        {
            if(_position >= _size)
                return -1;

            return this[_position++];
        }


        /// <summary>
        /// Asynchronously writes a sequence of bytes to the current stream, advances the current position within this stream by
        /// the number of bytes written, and monitors cancellation requests.
        /// </summary>
        /// <param name="buffer"> The buffer to write data from. </param>
        /// <param name="offset">
        /// The zero-based byte offset in <paramref name="buffer" /> from which to begin copying bytes to the
        /// stream.
        /// </param>
        /// <param name="count"> The maximum number of bytes to write. </param>
        /// <param name="cancellationToken">
        /// The token to monitor for cancellation requests. The default value is
        /// <see
        ///     cref="P:System.Threading.CancellationToken.None" />
        /// .
        /// </param>
        /// <returns> A task that represents the asynchronous write operation. </returns>
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            Write(buffer, offset, count);
            return TaskCache.CompletedTask;
        }

        /// <summary>
        /// When overridden in a derived class, writes a sequence of bytes to the current stream and advances the current position
        /// within this stream by the number of bytes written.
        /// </summary>
        /// <param name="buffer">
        /// An array of bytes. This method copies <paramref name="count" /> bytes from
        /// <paramref
        ///     name="buffer" />
        /// to the current stream.
        /// </param>
        /// <param name="offset">
        /// The zero-based byte offset in <paramref name="buffer" /> at which to begin copying bytes to the
        /// current stream.
        /// </param>
        /// <param name="count"> The number of bytes to be written to the current stream. </param>
        /// <filterpriority>1</filterpriority>
        public override void Write(byte[] buffer, int offset, int count)
        {
            WriteInternal(buffer, offset, count);
        }

        /// <summary>
        /// Writes the internal.
        /// </summary>
        /// <param name="buffer"> The buffer. </param>
        /// <param name="offset"> The offset. </param>
        /// <param name="count"> The count. </param>
        /// <exception cref="System.ObjectDisposedException">PoolMemoryStream</exception>
        private void WriteInternal(byte[] buffer, int offset, int count)
        {
            if(_disposed) throw new ObjectDisposedException("PoolMemoryStream");

            //allocate buffers if necessary
            while(_bufferCount*BufferSize <= _position + count)
                AddBuffer();

            //get location in buffer
            var bufferIndex = (int)(_position/BufferSize);
            var bufferOffset = (int)(_position%BufferSize);

            //move position ahead
            _position += count;

            //adjust sie
            _size = Math.Max(_size, _position);

            //copy data to internal buffers
            while(count > 0)
            {
                int copySize = Math.Min(count, BufferSize - bufferOffset);
                Buffer.BlockCopy(buffer, offset, _buffers[bufferIndex], bufferOffset, copySize);
                bufferIndex++;
                bufferOffset = 0;
                count -= copySize;
                offset += copySize;
            }
        }

        /// <summary>
        /// Writes a byte to the current position in the stream and advances the position within the stream by one byte.
        /// </summary>
        /// <param name="value"> The byte to write to the stream. </param>
        public override void WriteByte(byte value)
        {
            //allocate buffers if necessary
            if(_bufferCount*BufferSize <= _position + 1)
                AddBuffer();

            //get location in buffer
            var bufferIndex = (int)(_position/BufferSize);
            var bufferOffset = (int)(_position%BufferSize);

            //move position ahead
            _position++;

            //adjust size
            _size = Math.Max(_size, _position);

            //copy value to internal buffers
            _buffers[bufferIndex][bufferOffset] = value;
        }

        protected override void Dispose(bool disposing)
        {
            if(!disposing || _disposed)
                return;

            _disposed = true;

            for(int i = 0; i < _bufferCount; i++)
            {
                MemoryPool.Instance.Return(_buffers[i]);
                _buffers[i] = null;
            }
        }

        /// <summary>
        /// Reads the bytes from the current stream and writes them to another stream.
        /// </summary>
        /// <param name="destination"> The stream to which the contents of the current stream will be copied. </param>
        public new void CopyTo(Stream destination)
        {
            CopyTo(destination, BufferSize);
        }

        /// <summary>
        /// Reads the bytes from the current stream and writes them to another stream, using a specified buffer size.
        /// </summary>
        /// <param name="destination"> The stream to which the contents of the current stream will be copied. </param>
        /// <param name="bufferSize"> Ignored. The internal buffersize is always used. </param>
        /// <exception cref="System.ObjectDisposedException">PoolMemoryStream</exception>
        public new void CopyTo(Stream destination, int bufferSize)
        {
            if(_disposed) throw new ObjectDisposedException("PoolMemoryStream");

            long toCopy = _size - _position;

            var bufferIndex = (int)(_position/BufferSize);
            var bufferOffset = (int)(_position%BufferSize);

            while(toCopy > 0)
            {
                if(_disposed) throw new ObjectDisposedException("PoolMemoryStream");

                var writeSize = (int)Math.Min((long)BufferSize - bufferOffset, toCopy);
                destination.Write(_buffers[bufferIndex], bufferOffset, writeSize);
                bufferIndex++;
                bufferOffset = 0;
                toCopy -= writeSize;
            }
        }


        /// <summary>
        /// Asynchronously reads the bytes from the current stream and writes them to another stream, using a specified buffer size
        /// and cancellation token.
        /// </summary>
        /// <param name="destination"> The stream to which the contents of the current stream will be copied. </param>
        /// <param name="bufferSize">
        /// The size, in bytes, of the buffer. This value must be greater than zero. The default size is
        /// 4096.
        /// </param>
        /// <param name="cancellationToken">
        /// The token to monitor for cancellation requests. The default value is
        /// <see
        ///     cref="P:System.Threading.CancellationToken.None" />
        /// .
        /// </param>
        /// <returns> A task that represents the asynchronous copy operation. </returns>
        /// <exception cref="System.ObjectDisposedException">PoolMemoryStream</exception>
        public override async Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        {
            if(_disposed) throw new ObjectDisposedException("PoolMemoryStream");

            cancellationToken.ThrowIfCancellationRequested();

            long toCopy = _size - _position;

            var bufferIndex = (int)(_position/BufferSize);
            var bufferOffset = (int)(_position%BufferSize);

            while(toCopy > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if(_disposed) throw new ObjectDisposedException("PoolMemoryStream");

                var writeSize = (int)Math.Min((long)BufferSize - bufferOffset, toCopy);
                await destination.WriteAsync(_buffers[bufferIndex], bufferOffset, writeSize).AutoConfigureAwait();
                bufferIndex++;
                bufferOffset = 0;
                toCopy -= writeSize;
            }
        }
    }
}