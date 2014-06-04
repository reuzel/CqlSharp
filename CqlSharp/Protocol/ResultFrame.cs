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

            CqlResultType = (CqlResultType)await reader.ReadIntAsync().ConfigureAwait(false);
            switch (CqlResultType)
            {
                case CqlResultType.Void:
                    break;

                case CqlResultType.Rows:
                    ResultMetaData = await ReadMetaDataAsync().ConfigureAwait(false);
                    _count = await reader.ReadIntAsync().ConfigureAwait(false);
                    break;

                case CqlResultType.SetKeyspace:
                    Keyspace = await reader.ReadStringAsync().ConfigureAwait(false);
                    break;

                case CqlResultType.SchemaChange:
                    Change = await reader.ReadStringAsync().ConfigureAwait(false);
                    Keyspace = await reader.ReadStringAsync().ConfigureAwait(false);
                    Table = await reader.ReadStringAsync().ConfigureAwait(false);
                    break;

                case CqlResultType.Prepared:
                    PreparedQueryId = await reader.ReadShortBytesAsync().ConfigureAwait(false);
                    QueryMetaData = await ReadMetaDataAsync().ConfigureAwait(false);

                    //read result metadata if not version 1 
                    if ((Version & FrameVersion.ProtocolVersionMask) != FrameVersion.ProtocolVersion1)
                        ResultMetaData = await ReadMetaDataAsync().ConfigureAwait(false);

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
                valueBytes[i] = await Reader.ReadBytesAsync().ConfigureAwait(false);

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
            //await _readLock.WaitAsync().ConfigureAwait(false);
            //await Reader.BufferRemainingData().ConfigureAwait(false);
            //_readLock.Release();
            return Reader.BufferRemainingData();
        }

        private async Task<MetaData> ReadMetaDataAsync()
        {
            var metaData = new MetaData();

            FrameReader reader = Reader;

            //get flags
            var flags = (MetadataFlags)await reader.ReadIntAsync().ConfigureAwait(false);

            //get column count
            int colCount = await reader.ReadIntAsync().ConfigureAwait(false);

            //get paging state if present
            if (flags.HasFlag(MetadataFlags.HasMorePages))
            {
                metaData.PagingState = await reader.ReadBytesAsync().ConfigureAwait(false);
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
                keyspace = await reader.ReadStringAsync().ConfigureAwait(false);
                table = await reader.ReadStringAsync().ConfigureAwait(false);
            }

            //go and start processing all the columns
            for (int colIdx = 0; colIdx < colCount; colIdx++)
            {
                //read name
                string colKeyspace = globalTablesSpec ? keyspace : await reader.ReadStringAsync().ConfigureAwait(false);
                string colTable = globalTablesSpec ? table : await reader.ReadStringAsync().ConfigureAwait(false);
                string colName = await reader.ReadStringAsync().ConfigureAwait(false);

                //read typeCode
                var colType = (CqlTypeCode)await reader.ReadShortAsync().ConfigureAwait(false);
                CqlType type;
                switch (colType)
                {
                    case CqlTypeCode.Custom:
                        var colCustom = await reader.ReadStringAsync().ConfigureAwait(false);
                        type = CqlType.CreateType(colCustom);
                        break;

                    case CqlTypeCode.List:
                    case CqlTypeCode.Set:
                        var colValueType = (CqlTypeCode)await reader.ReadShortAsync().ConfigureAwait(false);
                        type = CqlType.CreateType(colType, CqlType.CreateType(colValueType));
                        break;

                    case CqlTypeCode.Map:
                        var colKeyType = (CqlTypeCode)await reader.ReadShortAsync().ConfigureAwait(false);
                        var colValType = (CqlTypeCode)await reader.ReadShortAsync().ConfigureAwait(false);
                        type = CqlType.CreateType(colType, CqlType.CreateType(colKeyType), CqlType.CreateType(colValType));
                        break;

                    default:
                        type = CqlType.CreateType(colType);
                        break;
                }

                //add to the MetaData
                metaData.Add(new Column(colIdx, colKeyspace, colTable, colName, type));
            }

            return metaData;
        }
    }
}