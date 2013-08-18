using CqlSharp.Serialization;
using System;
using System.Data;

namespace CqlSharp
{
    /// <summary>
    /// Represents a single parameter for use with CqlCommands
    /// </summary>
    public class CqlParameter : IDbDataParameter
    {
        public CqlType CqlType { get; set; }
        public CqlType? CollectionValueType { get; set; }
        public CqlType? CollectionKeyType { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlParameter" /> class.
        /// </summary>
        public CqlParameter()
        {
            IsNullable = true;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlParameter" /> class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="type">The type.</param>
        /// <param name="keyType">Type of the key (in case of a map).</param>
        /// <param name="valueType">Type of the value (in case of a map, set or list).</param>
        /// <param name="value">The value.</param>
        public CqlParameter(string name, CqlType type, CqlType? keyType = null, CqlType? valueType = null, object value = null)
        {
            ParameterName = name;
            CqlType = type;
            CollectionKeyType = keyType;
            CollectionValueType = valueType;
            Value = value;
            IsNullable = true;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlParameter" /> class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="type">The type.</param>
        /// <param name="value">The value.</param>
        public CqlParameter(string name, CqlType type, object value)
        {
            ParameterName = name;
            CqlType = type;
            Value = value;
            IsNullable = true;
        }

        /// <summary>
        /// Gets or sets the <see cref="T:System.Data.DbType" /> of the parameter.
        /// </summary>
        /// <returns>One of the <see cref="T:System.Data.DbType" /> values. The default is <see cref="F:System.Data.DbType.String" />.</returns>
        public DbType DbType
        {
            get { return CqlType.ToDbType(); }
            set { CqlType = value.ToCqlType(); }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value parameter.
        /// </summary>
        /// <returns>One of the <see cref="T:System.Data.ParameterDirection" /> values. The default is Input.</returns>
        /// <exception cref="System.NotSupportedException">Cql only supports input parameters</exception>
        public ParameterDirection Direction
        {
            get
            {
                return ParameterDirection.Input;
            }
            set
            {
                if (value != ParameterDirection.Input)
                    throw new NotSupportedException("Cql only supports input parameters");
            }
        }

        /// <summary>
        /// Gets a value indicating whether the parameter accepts null values.
        /// </summary>
        /// <returns>true if null values are accepted; otherwise, false. The default is false.</returns>
        public bool IsNullable { get; set; }

        /// <summary>
        /// Gets or sets the name of the <see cref="T:System.Data.IDataParameter" />.
        /// </summary>
        /// <returns>The name of the <see cref="T:System.Data.IDataParameter" />. The default is an empty string.</returns>
        public string ParameterName { get; set; }

        /// <summary>
        /// Gets or sets the name of the source column that is mapped to the <see cref="T:System.Data.DataSet" /> and used for loading or returning the <see cref="P:System.Data.IDataParameter.Value" />.
        /// </summary>
        /// <returns>The name of the source column that is mapped to the <see cref="T:System.Data.DataSet" />. The default is an empty string.</returns>
        public string SourceColumn { get; set; }

        /// <summary>
        /// Gets or sets the <see cref="T:System.Data.DataRowVersion" /> to use when loading <see cref="P:System.Data.IDataParameter.Value" />.
        /// </summary>
        /// <returns>One of the <see cref="T:System.Data.DataRowVersion" /> values. The default is Current.</returns>
        public DataRowVersion SourceVersion { get; set; }

        /// <summary>
        /// Gets or sets the value of the parameter.
        /// </summary>
        /// <returns>An <see cref="T:System.Object" /> that is the value of the parameter. The default value is null.</returns>
        public object Value { get; set; }

        /// <summary>
        /// Indicates the precision of numeric parameters.
        /// </summary>
        /// <returns>The maximum number of digits used to represent the Value property of a data provider Parameter object. The default value is 0, which indicates that a data provider sets the precision for Value.</returns>
        public byte Precision
        {
            get
            {
                if (Value != null)
                {
                    if (Value is float)
                        return 7;

                    if (Value is double)
                        return 16;
                }

                return 0;
            }
            set
            {
                //noop
            }
        }

        /// <summary>
        /// Indicates the scale of numeric parameters.
        /// </summary>
        /// <returns>The number of decimal places to which <see cref="T:System.Data.OleDb.OleDbParameter.Value" /> is resolved. The default is 0.</returns>
        public byte Scale
        {
            get
            {
                if (Value != null)
                {
                    if (Value is float || Value is double)
                    {
                        var val = (decimal)Value;
                        return (byte)((decimal.GetBits(val)[3] >> 16) & 0x7F);
                    }
                }

                return 0;
            }
            set
            {
                //noop
            }
        }

        /// <summary>
        /// The size of the parameter.
        /// </summary>
        /// <returns>The maximum size, in bytes, of the data within the column. The default value is inferred from the the parameter value.</returns>
        public int Size
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
    }
}