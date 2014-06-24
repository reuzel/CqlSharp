using CqlSharp.Serialization;
using CqlSharp.Serialization.Marshal;
using System;
using System.Collections.Generic;
using System.Dynamic;
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
    [CqlTypeConverter(typeof(UserDefinedConverter))]
    public class UserDefined: DynamicObject
    {
        public UserDefinedType Type { get; private set; }
        private object[] _values;

        public object[] Values
        {
            get { return _values; }
            set 
            {
                if (value.Length != Type.GetFieldCount())
                    throw new ArgumentException("Number of values provided does not match the number of fields in the UserDefinedType", "values");

                _values = value; 
            }
        }

        public UserDefined(UserDefinedType userDefinedType, object[] values)
        {
            Type = userDefinedType;

            if (values.Length != Type.GetFieldCount())
                throw new ArgumentException("Number of values provided does not match the number of fields in the UserDefinedType", "values");

            _values = values;
        }

        public UserDefined(UserDefinedType cqlType)
        {
            Type = cqlType;
            _values = new byte[Type.GetFieldCount()][];
        }

        public object this[string name]
        {
            get
            {
                int index = Type.GetFieldIndex(name);
                
                if(index < 0)
                    throw new ArgumentOutOfRangeException("name", "UserDefined object does not contain a field with name "+name);
                                
                return _values[index];
            }
            set
            {
                int index = Type.GetFieldIndex(name);

                if (index < 0)
                    throw new ArgumentOutOfRangeException("name", "UserDefined object does not contain a field with name " + name);

                _values[index] = value;
            }
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            string name = binder.Name;
            int index = Type.GetFieldIndex(name);
            if(index < 0)
            {
                result = null;
                return false;
            }

            result = _values[index];
            return true;
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            string name = binder.Name;
            int index = Type.GetFieldIndex(name);
            if (index < 0)
            {
                return false;
            }

            _values[index] = value;
            return true;
        }

       
    }
}
