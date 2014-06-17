using CqlSharp.Serialization;
using CqlSharp.Serialization.Marshal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp
{
    /// <summary>
    /// Represents an instance of a User Defined Type. Typically this object
    /// is not used directly but it is cast to a type holds each field as 
    /// a seperated property.
    /// </summary>
    public class UserDefined : ICastable
    {
        public UserDefinedType Type { get; private set; }
        private byte[][] _values;

        internal UserDefined(UserDefinedType userDefinedType, byte[][] rawValues)
        {
            Type = userDefinedType;
            _values = rawValues;
        }

        public T CastTo<T>()
	    {
            if (typeof(T) == typeof(UserDefined))
                return (T)(object)this;

            T result = Activator.CreateInstance<T>();

            foreach(CqlColumnInfo<T> column in ObjectAccessor<T>.Instance.Columns)
            {
                int index = Type.GetFieldIndex(column.Name);

                if (index >= 0)
                {
                    object value = Type.GetFieldType(index).Deserialize<object>(_values[index]);
                    object targetValue = Converter.ChangeType(value, column.Type);
                    column.WriteFunction(result, targetValue);
                }
            }

            return result;
	    }

        T GetValue<T>(int i)
        {
            return Type.GetFieldType(i).Deserialize<T>(_values[i]);
        }

        T GetValue<T>(string name)
        {
            int i = Type.GetFieldIndex(name);
            return GetValue<T>(i);
        }

        void SetValue<T>(int i, T value)
        {
            _values[i] = Type.GetFieldType(i).Serialize<T>(value);
        }

        internal byte[][] RawValues 
        { 
            get { return _values; } 
        }
    }
}
