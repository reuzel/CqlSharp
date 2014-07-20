// CqlSharp - CqlSharp
// Copyright (c) 2014 Joost Reuzel
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
using System.Data;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using CqlSharp.Annotations;
using CqlSharp.Protocol;

namespace CqlSharp.Serialization.Marshal
{
    /// <summary>
    /// Tuple type
    /// </summary>
    public class TupleType<T> : CqlType<T>
    {
        private static readonly Func<Stream, CqlType[], byte, T> Deserializer;

        private readonly CqlType[] _types;

        static TupleType()
        {
            var type = ValidateTypeArgument();

            Type[] typeArguments = type.GetGenericArguments();

            var data = Expression.Parameter(typeof(Stream));
            var types = Expression.Parameter(typeof(CqlType[]));
            var protocolVersion = Expression.Parameter(typeof(byte));

            //iterate over all items and convert the values
            var expressions = new Expression[typeArguments.Length];
            for(int i = 0; i < typeArguments.Length; i++)
            {
                var bytes = Expression.Call(typeof(StreamExtensions), "ReadByteArray", null, data);
                var cqlType = Expression.ArrayIndex(types, Expression.Constant(i));
                expressions[i] = Expression.Call(cqlType, "Deserialize", new[] { typeArguments[i] }, bytes, protocolVersion);
            }

            //create the new tupe
            var newTuple = Expression.Call(typeof(Tuple),
                                           "Create",
                                           expressions.Select(e => e.Type).ToArray(),
                                           expressions);

            Deserializer = Expression.Lambda<Func<Stream, CqlType[], byte, T>>(newTuple, data, types, protocolVersion).Compile();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TupleType{T}" /> class.
        /// </summary>
        [UsedImplicitly]
        public TupleType()
        {
            var type = ValidateTypeArgument();

            _types = type.GetGenericArguments()
                         .Select(CreateType)
                         .ToArray();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TupleType{T}" /> class.
        /// </summary>
        /// <param name="subTypes">The sub types.</param>
        public TupleType(IEnumerable<CqlType> subTypes)
        {
            var type = ValidateTypeArgument();

            _types = subTypes as CqlType[] ?? subTypes.ToArray();

            if(_types.Length != type.GetGenericArguments().Length)
            {
                throw new ArgumentException(
                    string.Format("Number of CqlTypes is incorrect. Should be {0} for a TupleType for {1}",
                                  type.GetGenericArguments().Length, type.Name));
            }
        }

        /// <summary>
        /// Validates the type argument.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="System.InvalidOperationException">Tuple type may only be constructed with System.Tuple subtypes</exception>
        private static Type ValidateTypeArgument()
        {
            var type = typeof(T);
            if(!type.IsGenericType || !TypeExtensions.TupleTypes.Contains(type.GetGenericTypeDefinition()))
                throw new InvalidOperationException("Tuple type may only be constructed with System.Tuple subtypes");

            return type;
        }

        /// <summary>
        /// Gets the CQL type code.
        /// </summary>
        /// <value>
        /// The CQL type code.
        /// </value>
        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Custom; }
        }

        /// <summary>
        /// Gets the name of the type.
        /// </summary>
        /// <param name="builder"></param>
        /// <returns>
        /// The name of the type.
        /// </returns>
        public override void AppendTypeName(StringBuilder builder)
        {
            builder.Append("org.apache.cassandra.db.marshal.TupleType(");
            for(int i = 0; i < _types.Length; i++)
            {
                if(i > 0) builder.Append(",");
                _types[i].AppendTypeName(builder);
            }
            builder.Append(")");
        }

        /// <summary>
        /// gets the corresponding the DbType
        /// </summary>
        /// <returns></returns>
        public override DbType ToDbType()
        {
            return DbType.Object;
        }

        /// <summary>
        /// Gets the maximum size in bytes of values of this type.
        /// </summary>
        /// <value>
        /// The maximum size in bytes.
        /// </value>
        public override int Size
        {
            get { return 2000000000; }
        }

        /// <summary>
        /// Serializes the specified value.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="protocolVersion"></param>
        /// <returns></returns>
        public override byte[] Serialize(T value, byte protocolVersion)
        {
            var accessor = ObjectAccessor<T>.Instance;
            using(var stream = new MemoryStream())
            {
                for(int i = 0; i < _types.Length; i++)
                {
                    ICqlColumnInfo<T> member;
                    if(accessor.ColumnsByName.TryGetValue("item" + (i + 1), out member))
                    {
                        byte[] itemData = member.SerializeFrom(value, _types[i], protocolVersion);
                        stream.WriteByteArray(itemData);
                    }
                }
                return stream.ToArray();
            }
        }

        /// <summary>
        /// Deserializes the specified data.
        /// </summary>
        /// <param name="data">The data.</param>
        /// <param name="protocolVersion"></param>
        /// <returns></returns>
        public override T Deserialize(byte[] data, byte protocolVersion)
        {
            using(var stream = new MemoryStream(data))
            {
                return Deserializer(stream, _types, protocolVersion);
            }
        }
    }
}