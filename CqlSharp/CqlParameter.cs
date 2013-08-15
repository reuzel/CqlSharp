using System;
using System.Data;
using CqlSharp.Serialization;

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

        public DbType DbType
        {
            get { return CqlType.ToDbType(); }
            set { CqlType = value.ToCqlType(); }
        }

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

        public bool IsNullable
        {
            get { return true; }
        }

        public string ParameterName { get; set; }

        public string SourceColumn { get; set; }

        public DataRowVersion SourceVersion { get; set; }

        public object Value { get; set; }

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