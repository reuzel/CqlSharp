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


// ReSharper disable CompareNonConstrainedGenericWithNull

using System;
using System.Linq.Expressions;
using System.Reflection;

namespace CqlSharp.Serialization
{
    /// <summary>
    /// Provides CQL information of a table class property or field
    /// </summary>
    /// <typeparam name="TTable"> The type of the table. </typeparam>
    /// <typeparam name="TMember"> The type of the class member that stores this column value</typeparam>
    internal class CqlColumnInfo<TTable, TMember> : ICqlColumnInfo<TTable>, IKeyMember
    {
        public CqlColumnInfo(MemberInfo member)
        {
            //set the member
            MemberInfo = member;

            //set the type
            Type = typeof(TMember);

            //check for column attribute
            var columnAttribute =
                Attribute.GetCustomAttribute(member, typeof(CqlColumnAttribute)) as CqlColumnAttribute;

            //get column name from attribute or base on name otherwise
            if(columnAttribute != null && columnAttribute.Column != null)
                Name = columnAttribute.Column;
            else
                Name = member.Name.ToLower();

            //get order if any
            if(columnAttribute != null && columnAttribute.OrderHasValue)
                Order = columnAttribute.Order;

            //get CqlTypeCode from attribute (if any)
            if(columnAttribute != null && columnAttribute.CqlTypeHasValue)
                CqlType = CqlType.CreateType(columnAttribute.CqlTypeCode);
            else
            {
                //get CqlTypeCode from property Type
                CqlType = CqlType.CreateType(Type);
            }

            //check for index attribute
            var indexAttribute =
                Attribute.GetCustomAttribute(member, typeof(CqlIndexAttribute)) as CqlIndexAttribute;

            if(indexAttribute != null)
            {
                IsIndexed = true;
                IndexName = indexAttribute.Name;
            }
            else
                IsIndexed = false;
        }

        /// <summary>
        /// The function to write to the member (compiled expression for performance reasons)
        /// </summary>
        private Action<TTable, TMember> _writeFunction;

        /// <summary>
        /// The function to read the member (compiled expression for performance reasons)
        /// </summary>
        private Func<TTable, TMember> _readFunction;

        /// <summary>
        /// Gets the column name.
        /// </summary>
        /// <value> The name. </value>
        public string Name { get; private set; }

        /// <summary>
        /// Gets the type of the CQL column.
        /// </summary>
        /// <value>
        /// The type of the CQL column.
        /// </value>
        public CqlType CqlType { get; private set; }

        /// <summary>
        /// Gets the .NET type.
        /// </summary>
        /// <value> The type. </value>
        public Type Type { get; private set; }

        /// <summary>
        /// Gets the order/index of a key column.
        /// </summary>
        /// <value> The order. </value>
        public int? Order { get; private set; }

        /// <summary>
        /// Gets a value indicating whether this column is part of the partition key.
        /// </summary>
        /// <value> <c>true</c> if this column is part of the partition key; otherwise, <c>false</c> . </value>
        public bool IsPartitionKey { get; set; }

        /// <summary>
        /// Gets a value indicating whether this column is part of the clustering key.
        /// </summary>
        /// <value> <c>true</c> if this column is part of the clustering key; otherwise, <c>false</c> . </value>
        public bool IsClusteringKey { get; set; }

        /// <summary>
        /// Gets a value indicating whether this column is indexed.
        /// </summary>
        /// <value> <c>true</c> if this column is indexed; otherwise, <c>false</c> . </value>
        public bool IsIndexed { get; private set; }

        /// <summary>
        /// Gets the name of the index (if any).
        /// </summary>
        /// <value> The name of the index. </value>
        public string IndexName { get; private set; }

        /// <summary>
        /// Gets the member information.
        /// </summary>
        /// <value> The member information. </value>
        public MemberInfo MemberInfo { get; private set; }


        /// <summary>
        /// Reads the column value from the specified source.
        /// </summary>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="source">The source.</param>
        /// <returns></returns>
        public TResult Read<TResult>(TTable source)
        {
            var value = ReadFunction(source);

            if(value == null)
                return default(TResult);

            return Converter.ChangeType<TMember, TResult>(value);
        }


        /// <summary>
        /// Gets the read function.
        /// </summary>
        /// <value>
        /// The read function.
        /// </value>
        private Func<TTable, TMember> ReadFunction
        {
            get
            {
                if(_readFunction == null)
                {
                    var source = Expression.Parameter(typeof(TTable));
                    var member = Expression.MakeMemberAccess(source, MemberInfo);

                    _readFunction = Expression.Lambda<Func<TTable, TMember>>(member, source).Compile();
                }

                return _readFunction;
            }
        }

        /// <summary>
        /// Writes the value to the column on the specified target.
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="target">The target.</param>
        /// <param name="value">The value.</param>
        /// <exception cref="System.ArgumentNullException">target</exception>
        public void Write<TValue>(TTable target, TValue value)
        {
            if(target == null)
                throw new ArgumentNullException("target");

            if(value == null)
                WriteFunction(target, default(TMember));
            else
            {
                var memberValue = Converter.ChangeType<TValue, TMember>(value);
                WriteFunction(target, memberValue);
            }


        }


        /// <summary>
        /// Gets the write function.
        /// </summary>
        /// <value>
        /// The write function.
        /// </value>
        private Action<TTable, TMember> WriteFunction
        {
            get
            {
                if(_writeFunction == null)
                {
                    var target = Expression.Parameter(typeof(TTable));
                    var value = Expression.Parameter(typeof(TMember));
                    var member = Expression.MakeMemberAccess(target, MemberInfo);

                    Expression body = Expression.Assign(member, value);

                    _writeFunction = Expression.Lambda<Action<TTable, TMember>>(body, target, value).Compile();
                }

                return _writeFunction;
            }
        }


        /// <summary>
        /// Serializes the column value from the provided source using the given type.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public byte[] SerializeFrom(TTable source, CqlType type)
        {
            TMember value = ReadFunction(source);
            return value == null ? null : type.Serialize(value);
        }

        /// <summary>
        /// Deserializes the provided data using the given type and assigns it to the column member of the given target.
        /// </summary>
        /// <param name="target">The target.</param>
        /// <param name="data">The data.</param>
        /// <param name="type">The type of the data.</param>
        public void DeserializeTo(TTable target, byte[] data, CqlType type)
        {
            TMember value = data != null ? type.Deserialize<TMember>(data) : default(TMember);
            WriteFunction(target, value);
        }

        /// <summary>
        /// Writes a value to the member belonging to this column on the specified target.
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="target">The target.</param>
        /// <param name="value">The value.</param>
        void ICqlColumnInfo.Write<TValue>(object target, TValue value)
        {
            Write((TTable)target, value);
        }

        /// <summary>
        /// Reads a value from the member belonging to this column from the specified source.
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        TValue ICqlColumnInfo.Read<TValue>(object source)
        {
            return Read<TValue>((TTable)source);
        }


        /// <summary>
        /// Serializes the column value from the provided source using the given type.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        byte[] ICqlColumnInfo.SerializeFrom(object source, CqlType type)
        {
            return SerializeFrom((TTable)source, type);
        }

        /// <summary>
        /// Deserializes the provided data using the given type and assigns it to the column member of the given target.
        /// </summary>
        /// <param name="target">The target.</param>
        /// <param name="data">The data.</param>
        /// <param name="type">The type of the data.</param>
        void ICqlColumnInfo.DeserializeTo(object target, byte[] data, CqlType type)
        {
            DeserializeTo((TTable)target, data, type);
        }
    }
}