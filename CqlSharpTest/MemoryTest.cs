// CqlSharp - CqlSharp.Test
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

using CqlSharp.Memory;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Linq;

namespace CqlSharp.Test
{
    [TestClass]
    public class MemoryTest
    {
        [TestMethod]
        public void PoolMemoryStreamWriteBytes()
        {
            //assume
            const int count = 20000;
            var bytes = new byte[count];
            for (int i = 0; i < count; i++)
                bytes[i] = (byte)((i - byte.MinValue) % byte.MaxValue);

            //act
            var stream = new PoolMemoryStream();
            stream.Write(bytes, 0, count);

            //assert
            Assert.AreEqual(stream.Length, count, "Unexpected stream length");
            Assert.AreEqual(stream.Position, count, "Unexpected position");
            Assert.IsTrue(stream.Capacity >= count, "Unexpected capacity");
        }

        [TestMethod]
        public void PoolMemoryStreamReadWriteBytes()
        {
            //assume
            const int count = 20000;
            var bytes = new byte[count];
            for (int i = 0; i < count; i++)
                bytes[i] = (byte)((i - byte.MinValue) % byte.MaxValue);

            //act
            var stream = new PoolMemoryStream();
            stream.Write(bytes, 0, count);
            stream.Position = 0;

            var readBytes = new byte[count];
            int actual = 0;
            int read = 0;
            do
            {
                read = stream.Read(readBytes, read, count - read);
                actual += read;
            } while (read > 0);

            //assert
            Assert.AreEqual(stream.Length, actual, "not all written bytes can be read");
            Assert.IsTrue(readBytes.SequenceEqual(bytes), "returned bytes differ from written bytes");


            stream.Dispose();
        }

        [TestMethod]
        public void PoolMemoryStreamWriteCopyToAsync()
        {
            //assume
            const int count = 20000;
            var bytes = new byte[count];
            for (int i = 0; i < count; i++)
                bytes[i] = (byte)((i - byte.MinValue) % byte.MaxValue);

            //act
            var stream = new PoolMemoryStream();
            stream.Write(bytes, 0, count);
            stream.Position = 0;

            var target = new MemoryStream();
            stream.CopyToAsync(target).Wait();

            var readBytes = target.ToArray();

            //assert
            Assert.AreEqual(stream.Length, readBytes.Length, "not all written bytes are copied");
            Assert.IsTrue(readBytes.SequenceEqual(bytes), "returned bytes differ from written bytes");
        }

        [TestMethod]
        public void PoolMemoryStreamReadWriteSingleBytes()
        {
            //assume
            const int count = 20000;
            var bytes = new byte[count];
            for (int i = 0; i < count; i++)
                bytes[i] = (byte)((i - byte.MinValue) % byte.MaxValue);

            //act
            var stream = new PoolMemoryStream();
            for (int i = 0; i < count; i++)
                stream.WriteByte(bytes[i]);

            stream.Position = 0;

            Assert.AreEqual(stream.Length, count, "Unexpected stream length!");
            for (int i = 0; i < count; i++)
                Assert.AreEqual((byte)stream.ReadByte(), bytes[i], "Read byte is not equal to written bytes!");
        }
    }
}