// CqlSharp - CqlSharp
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

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp.Protocol
{
    internal class ResultFrame : Frame
    {
        private volatile int _count;
        private SemaphoreSlim _readLock;

        public ResultOpcode ResultOpcode { get; private set; }

        public int Count
        {
            get { return _count; }
        }

        public CqlSchema Schema { get; private set; }

        public byte[] PreparedQueryId { get; private set; }

        public string Change { get; private set; }

        public string Keyspace { get; private set; }

        public string Table { get; private set; }

        protected override void WriteData(Stream buffer)
        {
            throw new NotSupportedException();
        }

        protected override async Task InitializeAsync()
        {
            FrameReader reader = Reader;

            ResultOpcode = (ResultOpcode) await reader.ReadIntAsync();
            switch (ResultOpcode)
            {
                case ResultOpcode.Void:
                    break;

                case ResultOpcode.Rows:
                    Schema = await ReadCqlSchemaAsync();
                    _count = await reader.ReadIntAsync();
                    _readLock = new SemaphoreSlim(1);
                    break;

                case ResultOpcode.SetKeyspace:
                    Keyspace = await reader.ReadStringAsync();
                    break;

                case ResultOpcode.SchemaChange:
                    Change = await reader.ReadStringAsync();
                    Keyspace = await reader.ReadStringAsync();
                    Table = await reader.ReadStringAsync();
                    break;

                case ResultOpcode.Prepared:
                    PreparedQueryId = await reader.ReadShortBytesAsync();
                    Schema = await ReadCqlSchemaAsync();
                    break;

                default:
                    throw new ArgumentException("Unexpected ResultOpcode");
            }
        }

        public async Task<byte[][]> ReadNextDataRowAsync()
        {
            if (_count == 0)
                return null;

            await _readLock.WaitAsync();


            var valueBytes = new byte[Schema.Count][];
            for (int i = 0; i < Schema.Count; i++)
                valueBytes[i] = await Reader.ReadBytesAsync();

            //reduce the amount of available rows
            _count--;

            //dispose reader when it is no longer needed
            if (_count == 0)
                Reader.Dispose();

            _readLock.Release();

            return valueBytes;
        }

        public async Task BufferDataAsync()
        {
            await _readLock.WaitAsync();
            await Reader.BufferRemainingData();
            _readLock.Release();
        }

        internal async Task<CqlSchema> ReadCqlSchemaAsync()
        {
            FrameReader reader = Reader;
            var flags = (MetadataFlags) await reader.ReadIntAsync();
            bool globalTablesSpec = 0 != (flags & MetadataFlags.GlobalTablesSpec);

            int colCount = await reader.ReadIntAsync();

            string keyspace = null;
            string table = null;
            if (globalTablesSpec)
            {
                keyspace = await reader.ReadStringAsync();
                table = await reader.ReadStringAsync();
            }

            var columnSpecs = new List<CqlColumn>(colCount);
            for (int colIdx = 0; colIdx < colCount; ++colIdx)
            {
                string colKeyspace = keyspace;
                string colTable = table;
                if (!globalTablesSpec)
                {
                    colKeyspace = await reader.ReadStringAsync();
                    colTable = await reader.ReadStringAsync();
                }
                string colName = await reader.ReadStringAsync();
                var colType = (CqlType) await reader.ReadShortAsync();
                string colCustom = null;
                CqlType? colKeyType = null;
                CqlType? colValueType = null;
                switch (colType)
                {
                    case CqlType.Custom:
                        colCustom = await reader.ReadStringAsync();
                        break;

                    case CqlType.List:
                    case CqlType.Set:
                        colValueType = (CqlType) await reader.ReadShortAsync();
                        break;

                    case CqlType.Map:
                        colKeyType = (CqlType) await reader.ReadShortAsync();
                        colValueType = (CqlType) await reader.ReadShortAsync();
                        break;
                }

                columnSpecs.Add(new CqlColumn(colIdx, colKeyspace, colTable, colName, colType, colCustom,
                                              colKeyType, colValueType));
            }

            return new CqlSchema(columnSpecs);
        }
    }
}