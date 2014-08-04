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
using System.IO;
using System.Threading.Tasks;

namespace CqlSharp.Protocol
{
    internal class ResultFrame : Frame
    {
        private volatile int _count;

        public CqlResultType CqlResultType { get; private set; }

        public int Count
        {
            get { return _count; }
        }

        public MetaData QueryMetaData { get; private set; }

        public MetaData ResultMetaData { get; set; }

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

            CqlResultType = (CqlResultType) await reader.ReadIntAsync();
            switch (CqlResultType)
            {
                case CqlResultType.Void:
                    break;

                case CqlResultType.Rows:
                    ResultMetaData = await ReadMetaDataAsync();
                    _count = await reader.ReadIntAsync();
                    break;

                case CqlResultType.SetKeyspace:
                    Keyspace = await reader.ReadStringAsync();
                    break;

                case CqlResultType.SchemaChange:
                    Change = await reader.ReadStringAsync();
                    Keyspace = await reader.ReadStringAsync();
                    Table = await reader.ReadStringAsync();
                    break;

                case CqlResultType.Prepared:
                    PreparedQueryId = await reader.ReadShortBytesAsync();
                    QueryMetaData = await ReadMetaDataAsync();

                    //read result metadata if not version 1 
                    if ((Version & FrameVersion.ProtocolVersionMask) != FrameVersion.ProtocolVersion1)
                        ResultMetaData = await ReadMetaDataAsync();

                    break;

                default:
                    throw new ArgumentException("Unexpected ResultOpcode");
            }
        }

        public async Task<byte[][]> ReadNextDataRowAsync()
        {
            if (_count == 0)
                return null;

            var valueBytes = new byte[ResultMetaData.Count][];
            for (int i = 0; i < ResultMetaData.Count; i++)
                valueBytes[i] = await Reader.ReadBytesAsync();

            //reduce the amount of available rows
            _count--;

            //dispose reader when it is no longer needed
            if (_count == 0)
                Reader.Dispose();

            //_readLock.Release();

            return valueBytes;
        }

        public Task BufferDataAsync()
        {
            //await _readLock.WaitAsync();
            //await Reader.BufferRemainingData();
            //_readLock.Release();
            return Reader.BufferRemainingData();
        }

        internal async Task<MetaData> ReadMetaDataAsync()
        {
            var metaData = new MetaData();

            FrameReader reader = Reader;

            //get flags
            var flags = (MetadataFlags) await reader.ReadIntAsync();

            //get column count
            int colCount = await reader.ReadIntAsync();

            //get paging state if present
            if (flags.HasFlag(MetadataFlags.HasMorePages))
            {
                metaData.PagingState = await reader.ReadBytesAsync();
            }

            //stop processing if no metadata flag is set
            if (flags.HasFlag(MetadataFlags.NoMetaData))
                return metaData;

            //get the global keyspace,table if present
            bool globalTablesSpec = flags.HasFlag(MetadataFlags.GlobalTablesSpec);
            string keyspace = null;
            string table = null;
            if (globalTablesSpec)
            {
                keyspace = await reader.ReadStringAsync();
                table = await reader.ReadStringAsync();
            }

            //go and start processing all the columns
            for (int colIdx = 0; colIdx < colCount; colIdx++)
            {
                //read name
                string colKeyspace = globalTablesSpec ? keyspace : await reader.ReadStringAsync();
                string colTable = globalTablesSpec ? table : await reader.ReadStringAsync();
                string colName = await reader.ReadStringAsync();

                //read type
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

                //add to the MetaData
                metaData.Add(new Column(colIdx, colKeyspace, colTable, colName, colType, colCustom,
                                        colKeyType, colValueType));
            }

            return metaData;
        }
    }
}