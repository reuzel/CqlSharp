using CqlSharp.Serialization.Marshal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Serialization
{
    interface ITypeConverter<TType>
    {
        TTarget ConvertTo<TTarget>(TType source);

        TType ConvertFrom<TSource>(TSource source);
    }

    class UserDefinedConverter : ITypeConverter<UserDefined>
    {
        public TTarget ConvertTo<TTarget>(UserDefined source)
        {
            TTarget instance = Activator.CreateInstance<TTarget>();
            var accessor = ObjectAccessor<TTarget>.Instance;

            int count = source.Type.GetFieldCount();
            for (int i = 0; i < count; i++)
            {
                ICqlColumnInfo<TTarget> column;
                if (accessor.ColumnsByName.TryGetValue(source.Type.GetFieldName(i), out column))
                {
                    object value = Converter.ChangeType(source.Values[i], column.Type);
                    column.Write(instance, value);
                }
            }

            return instance;
        }

        public UserDefined ConvertFrom<TSource>(TSource source)
        {
            var type = CqlType.CreateType(typeof(TSource)) as UserDefinedType;

            if(type==null)
                throw new ArgumentException("Source must be mapped to a UserDefinedType to have it converted");

            var accessor = ObjectAccessor<TSource>.Instance;

            int count = type.GetFieldCount();
            object[] values = new object[count];

            for (int i = 0; i < count; i++)
            {
                ICqlColumnInfo<TSource> column;
                if (accessor.ColumnsByName.TryGetValue(type.GetFieldName(i), out column))
                {
                    values[i] = column.Read<object>(source);
                }
            }

            return new UserDefined(type, values);

        }
    }
}
