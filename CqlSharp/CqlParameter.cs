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
using System.Data;
using System.Data.Common;
using CqlSharp.Protocol;
using CqlSharp.Serialization;

namespace CqlSharp
{
    /// <summary>
    ///   Represents a single parameter for use with CqlCommands
    /// </summary>
    public class CqlParameter : DbParameter
    {
        private static readonly char[] TableSeperator = new[] {'.'};
        private readonly Column _column;
        private bool _isNullable;
        private object _value;

        internal CqlParameter(Column column)
        {
            _column = column;
            _isNullable = true;
            IsFixed = true;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlParameter" /> class.
        /// </summary>
        public CqlParameter()
        {
            _column = new Column();
            _isNullable = true;
            IsFixed = false;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlParameter" /> class.
        /// </summary>
        /// <param name="name"> The name. </param>
        public CqlParameter(string name)
            : this()
        {
            SetParameterName(name);
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlParameter" /> class.
        ///   The type of the parameter will be guessed from the value.
        /// </summary>
        /// <param name="name"> The name. </param>
        /// <param name="value"> The value. </param>
        public CqlParameter(string name, object value)
            : this()
        {
            SetParameterName(name);
            _column.GuessType(value.GetType());
            _value = value;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlParameter" /> class.
        /// </summary>
        /// <param name="name"> The name. </param>
        /// <param name="type"> The type. </param>
        /// <param name="keyType"> Type of the key (in case of a map). </param>
        /// <param name="valueType"> Type of the value (in case of a map, set or list). </param>
        public CqlParameter(string name, CqlType type, CqlType? keyType = null, CqlType? valueType = null)
            : this()
        {
            SetParameterName(name);
            CqlType = type;
            CollectionKeyType = keyType;
            CollectionValueType = valueType;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlParameter" /> class.
        /// </summary>
        /// <param name="table"> The table. </param>
        /// <param name="name"> The name. </param>
        /// <param name="type"> The type. </param>
        /// <param name="keyType"> Type of the key (in case of a map). </param>
        /// <param name="valueType"> Type of the value (in case of a map, set or list). </param>
        public CqlParameter(string table, string name, CqlType type, CqlType? keyType = null, CqlType? valueType = null)
            : this()
        {
            ColumnName = name;
            Table = table;
            CqlType = type;
            CollectionKeyType = keyType;
            CollectionValueType = valueType;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlParameter" /> class.
        /// </summary>
        /// <param name="keyspace"> The keyspace. </param>
        /// <param name="table"> The table. </param>
        /// <param name="name"> The name. </param>
        /// <param name="type"> The type. </param>
        /// <param name="keyType"> Type of the key (in case of a map). </param>
        /// <param name="valueType"> Type of the value (in case of a map, set or list). </param>
        public CqlParameter(string keyspace, string table, string name, CqlType type, CqlType? keyType = null,
                            CqlType? valueType = null)
            : this()
        {
            Keyspace = keyspace;
            Table = table;
            ColumnName = name;
            CqlType = type;
            CollectionKeyType = keyType;
            CollectionValueType = valueType;
        }

        /// <summary>
        ///   Gets a value indicating whether this paramater is fixed, implying that type and name can no
        ///   longer be changed.
        /// </summary>
        /// <value> <c>true</c> if [is fixed]; otherwise, <c>false</c> . </value>
        public bool IsFixed { get; internal set; }

        /// <summary>
        ///   Gets the column .
        /// </summary>
        /// <value> The column. </value>
        internal Column Column
        {
            get { return _column; }
        }

        /// <summary>
        ///   Gets or sets the type of the column.
        /// </summary>
        /// <value> The type of the CQL. </value>
        public CqlType CqlType
        {
            get { return _column.CqlType; }
            set
            {
                CheckFixed();

                _column.CqlType = value;
            }
        }

        /// <summary>
        ///   Gets or sets the type of the collection value.
        /// </summary>
        /// <value> The type of the collection value. </value>
        public CqlType? CollectionValueType
        {
            get { return _column.CollectionValueType; }
            set
            {
                CheckFixed();
                _column.CollectionValueType = value;
            }
        }

        /// <summary>
        ///   Gets or sets the type of the collection key.
        /// </summary>
        /// <value> The type of the collection key. </value>
        public CqlType? CollectionKeyType
        {
            get { return _column.CollectionKeyType; }
            set
            {
                CheckFixed();
                _column.CollectionKeyType = value;
            }
        }

        /// <summary>
        ///   Gets or sets the name of the column.
        /// </summary>
        /// <value> The name of the column. </value>
        public string ColumnName
        {
            get { return _column.Name; }
            set
            {
                CheckFixed();
                _column.Name = value;
            }
        }

        /// <summary>
        ///   Gets or sets the table.
        /// </summary>
        /// <value> The table. </value>
        public string Table
        {
            get { return _column.Table; }
            set
            {
                CheckFixed();
                _column.Table = value;
            }
        }

        /// <summary>
        ///   Gets or sets the keyspace.
        /// </summary>
        /// <value> The keyspace. </value>
        public string Keyspace
        {
            get { return _column.Keyspace; }
            set
            {
                CheckFixed();
                _column.Keyspace = value;
            }
        }

        /// <summary>
        ///   Gets or sets the <see cref="T:System.Data.DbType" /> of the parameter.
        /// </summary>
        /// <returns> One of the <see cref="T:System.Data.DbType" /> values. The default is <see cref="F:System.Data.DbType.String" /> . </returns>
        public override DbType DbType
        {
            get { return CqlType.ToDbType(); }
            set
            {
                CheckFixed();
                CqlType = value.ToCqlType();
            }
        }

        /// <summary>
        ///   Gets or sets a value indicating whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value parameter.
        /// </summary>
        /// <returns> One of the <see cref="T:System.Data.ParameterDirection" /> values. The default is Input. </returns>
        /// <exception cref="System.NotSupportedException">Cql only supports input parameters</exception>
        public override ParameterDirection Direction
        {
            get { return ParameterDirection.Input; }
            set
            {
                if (value != ParameterDirection.Input)
                    throw new NotSupportedException("Cql only supports input parameters");
            }
        }

        /// <summary>
        ///   Gets a value indicating whether the parameter accepts null values.
        /// </summary>
        /// <returns> true if null values are accepted; otherwise, false. The default is false. </returns>
        public override bool IsNullable
        {
            get { return _isNullable; }
            set { _isNullable = value; }
        }

        /// <summary>
        ///   Gets or sets the name of the <see cref="T:System.Data.IDataParameter" />. The name will be parsed to
        ///   attempt to derive keyspace and table information from it. Eg."test.dummies" will be parsed in ColumnName dummies and KeySpace test
        /// </summary>
        /// <returns> The name of the <see cref="T:System.Data.IDataParameter" /> . The default is an empty string. </returns>
        public override string ParameterName
        {
            get { return _column.KeySpaceTableAndName; }
            set
            {
                CheckFixed();
                SetParameterName(value);
            }
        }

        /// <summary>
        ///   Gets or sets the name of the source column that is mapped to the <see cref="T:System.Data.DataSet" /> and used for loading or returning the <see
        ///    cref="P:System.Data.IDataParameter.Value" />.
        /// </summary>
        /// <returns> The name of the source column that is mapped to the <see cref="T:System.Data.DataSet" /> . The default is an empty string. </returns>
        public override string SourceColumn { get; set; }

        /// <summary>
        ///   Gets or sets the <see cref="T:System.Data.DataRowVersion" /> to use when loading <see
        ///    cref="P:System.Data.IDataParameter.Value" />.
        /// </summary>
        /// <returns> One of the <see cref="T:System.Data.DataRowVersion" /> values. The default is Current. </returns>
        public override DataRowVersion SourceVersion { get; set; }

        /// <summary>
        ///   Gets or sets the value of the parameter. If no type information was provided earlier, the type of the parameter will be
        ///   guessed from the value's type.
        /// </summary>
        /// <returns> An <see cref="T:System.Object" /> that is the value of the parameter. The default value is null. </returns>
        public override object Value
        {
            get { return _value; }
            set { _value = value; }
        }

        /// <summary>
        ///   The size of the parameter.
        /// </summary>
        /// <returns> The maximum size, in bytes, of the data within the column. The default value is inferred from the the parameter value. </returns>
        public override int Size
        {
            get
            {
                if (Value == null)
                    return 0;

                return ValueSerialization.Serialize(CqlType, CollectionKeyType, CollectionValueType, Value).Length;
            }
            set
            {
                //noop
            }
        }

        /// <summary>
        ///   Sets or gets a value which indicates whether the source column is nullable. This allows <see
        ///    cref="T:System.Data.Common.DbCommandBuilder" /> to correctly generate Update statements for nullable columns.
        /// </summary>
        /// <returns> true if the source column is nullable; false if it is not. </returns>
        /// <exception cref="System.NotImplementedException"></exception>
        public override bool SourceColumnNullMapping
        {
            get { return IsNullable; }
            set { IsNullable = value; }
        }

        /// <summary>
        ///   Checks if the type or name of the parameter may be changed
        /// </summary>
        /// <exception cref="System.InvalidOperationException">Can't change the type or name of a CqlParameter after it has been prepared, or used with a query</exception>
        private void CheckFixed()
        {
            if (IsFixed)
                throw new InvalidOperationException(
                    "Can't change the type or name of a CqlParameter after it has been used to prepare or run a query");
        }

        /// <summary>
        ///   Sets the name of the parameter.
        /// </summary>
        /// <param name="value"> The value. </param>
        private void SetParameterName(string value)
        {
            string[] parts = value.Split(TableSeperator, 3, StringSplitOptions.RemoveEmptyEntries);
            int count = parts.Length;
            _column.Name = parts[count - 1];
            _column.Table = count > 1 ? parts[count - 2] : null;
            _column.Keyspace = count > 2 ? parts[count - 3] : null;
        }

        /// <summary>
        ///   Resets the DbType property to its original settings.
        /// </summary>
        /// <exception cref="System.NotImplementedException"></exception>
        public override void ResetDbType()
        {
        }
    }
}