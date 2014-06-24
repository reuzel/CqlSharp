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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace CqlSharp.Serialization
{
    /// <summary>
    ///   Provides a generic conversion routine between types.
    /// </summary>
    public static class Converter
    {
        /// <summary>
        ///   The conversion methods provided by the IConvertible interface. Used by the different TypeConverter classes
        /// </summary>
        private static readonly Dictionary<Type, MethodInfo> ConversionMethods =
            typeof(IConvertible).GetMethods()
                .Where(m => m.Name.StartsWith("To") && m.Name != "ToType")
                .ToDictionary(m => m.ReturnType, m => m);

        /// <summary>
        ///   Changes the type. Uses IConvertible methods when source implements them
        /// </summary>
        /// <typeparam name="TS"> The type of the struct or class to be converted/casted </typeparam>
        /// <typeparam name="TT"> The type of struct or class to which the source must be converted or casted </typeparam>
        /// <param name="source"> The object or value to be casted </param>
        /// <returns> an object or value type to which the source was converted </returns>
        public static TT ChangeType<TS, TT>(TS source)
        {
            return TypeConverter<TS, TT>.Convert(source);
        }

        private static ConcurrentDictionary<Tuple<Type, Type>, Func<object, object>> _conversionFunctions = new ConcurrentDictionary<Tuple<Type, Type>, Func<object, object>>();
        /// <summary>
        /// Changes the type of the given object to the specific type.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="targetType">Type of the target.</param>
        /// <returns>an object of the given targetType</returns>
        public static object ChangeType(object source, Type targetType)
        {
            var types = Tuple.Create(source.GetType(), targetType);

            var converter = _conversionFunctions.GetOrAdd(types, ts =>
                {
                    var src = Expression.Parameter(typeof(object));
                    var converted = Expression.Convert(src, ts.Item1);
                    var call = Expression.Call(typeof(Converter), "ChangeType", new[] { ts.Item1, ts.Item2 }, converted);
                    var result = Expression.Convert(call, typeof(object));
                    var lambda = Expression.Lambda<Func<object, object>>(result, src);
                    return lambda.Compile();
                });

            return converter(source);
        }
                

        #region Nested type: TypeConverter

        /// <summary>
        ///   Container class for the conversion routine between two specific types
        /// </summary>
        /// <typeparam name="TSource"> The type of the source. </typeparam>
        /// <typeparam name="TTarget"> The type of the target. </typeparam>
        private static class TypeConverter<TSource, TTarget>
        {
            /// <summary>
            ///   The conversion function.
            /// </summary>
            public static readonly Func<TSource, TTarget> Convert;

            /// <summary>
            ///   Initializes the <see cref="TypeConverter{TSource, TTarget}" /> conversion routine.
            /// </summary>
            static TypeConverter()
            {
                //get the types of source and target
                var srcType = typeof(TSource);
                var targetType = typeof(TTarget);

                //start with creating a src parameter as function input
                var src = Expression.Parameter(srcType);

                //create function call expression
                Expression call = GetIdentityConversion(srcType, targetType, src) ??
                                  GetCastConversion(targetType, src) ??
                                  GetIConvertibleConversion(srcType, targetType, src) ??
                                  GetICastableConversion(srcType, targetType, src) ??
                                  GetITypeConverterConversion(srcType, targetType, src) ??
                                  GetCollectionConversion(srcType, targetType, src) ??
                                  GetCopyConstructorConversion(srcType, targetType, src) ??
                                  GetToStringConversion(srcType, targetType, src) ??
                                  GetInvalidConversion(srcType, targetType);

                //translate the call into a lamda and compile it
                var lambda = Expression.Lambda<Func<TSource, TTarget>>(call,
                                                                       "Convert" + srcType.Name + "To" + targetType.Name,
                                                                       new[] { src });

                Debug.WriteLine(lambda.Name + " : " + lambda);

                Convert = lambda.Compile();
            }

            /// <summary>
            ///   Gets the identity conversion, where src is returned when src and target types are identical
            /// </summary>
            /// <param name="srcType"> Type of the source. </param>
            /// <param name="targetType"> Type of the target. </param>
            /// <param name="src"> The source. </param>
            /// <returns> </returns>
            private static Expression GetIdentityConversion(Type srcType, Type targetType, ParameterExpression src)
            {
                return srcType == targetType ? src : null;
            }

            /// <summary>
            ///   Gets the cast conversion expression, where conversion occurs by a direct checked cast between the types
            /// </summary>
            /// <param name="targetType"> Type of the target. </param>
            /// <param name="src"> The source. </param>
            /// <returns> </returns>
            private static Expression GetCastConversion(Type targetType, ParameterExpression src)
            {
                //try casting directly
                try
                {
                    return Expression.ConvertChecked(src, targetType);
                }
                catch
                {
                    //ugly: flow control through exception, but this should occur only once per certain conversion
                    //and there is no good CanCastTo method on Type defined...

                    return null;
                }
            }

            /// <summary>
            ///   Gets a conversion expression using the IConvirtable implementation of the source (if any).
            /// </summary>
            /// <param name="srcType"> Type of the source. </param>
            /// <param name="targetType"> Type of the target. </param>
            /// <param name="src"> The source. </param>
            /// <returns> </returns>
            private static Expression GetIConvertibleConversion(Type srcType, Type targetType, ParameterExpression src)
            {
                Expression call = null;

                //Check if type implements IConvertible
                if (typeof(IConvertible).IsAssignableFrom(srcType))
                {
                    //remove any nullable wrappers on the target, increasing the chance of finding the right conversion method
                    var nonNullableTargetType = Nullable.GetUnderlyingType(targetType) ?? targetType;

                    //check if one of the conversion methods can be used
                    MethodInfo method;
                    if (ConversionMethods.TryGetValue(nonNullableTargetType, out method))
                    {
                        call = Expression.Call(src, method, Expression.Constant(null, typeof(IFormatProvider)));

                        //convert result back to nullable value if necessary
                        if (nonNullableTargetType != targetType)
                            call = Expression.Convert(call, targetType);
                    }
                    else
                    {
                        //use the generic ToType method.
                        method = typeof(IConvertible).GetMethod("ToType");
                        call = Expression.Convert(
                            Expression.Call(src,
                                            method,
                                            Expression.Constant(targetType),
                                            Expression.Constant(null, typeof(IFormatProvider))),
                            targetType);
                    }
                }

                return call != null ? AddNullCheck(srcType, targetType, src, call) : null;
            }

            /// <summary>
            ///  Gets a cast expression that utilizes the ICastable implementation of the source
            /// </summary>
            /// <param name="srcType">Type of the source.</param>
            /// <param name="targetType">Type of the target.</param>
            /// <param name="src">The source.</param>
            /// <returns></returns>
            private static Expression GetICastableConversion(Type srcType, Type targetType, ParameterExpression src)
            {
                if(srcType.GetInterfaces().Contains(typeof(ICastable)))
                {
                    var castMethod = typeof(ICastable).GetMethod("CastTo");
                    var genericMethod = castMethod.MakeGenericMethod(targetType);

                    return AddNullCheck(srcType, targetType, src, Expression.Call(src, genericMethod));
                }

                return null;
            }

            private static Expression GetITypeConverterConversion(Type srcType, Type targetType, ParameterExpression src)
            {
                var converterAttribute = Attribute.GetCustomAttribute(srcType, typeof(CqlTypeConverterAttribute)) as CqlTypeConverterAttribute;

                if(converterAttribute!=null)
                {
                    var converterType = converterAttribute.Converter;
                    var converter = Expression.Constant(Activator.CreateInstance(converterType));
                    var call = Expression.Call(converter, "ConvertTo", new[] { targetType }, src);
                    return AddNullCheck(srcType, targetType, src, call);
                }

                converterAttribute = Attribute.GetCustomAttribute(targetType, typeof(CqlTypeConverterAttribute)) as CqlTypeConverterAttribute;

                if (converterAttribute != null)
                {
                    var converterType = converterAttribute.Converter;
                    var converter = Expression.Constant(Activator.CreateInstance(converterType));
                    var call = Expression.Call(converter, "ConvertFrom", new[] { srcType }, src);
                    return AddNullCheck(srcType, targetType, src, call);
                }

                return null;
            }

            private static Expression GetCollectionConversion(Type srcType, Type targetType, ParameterExpression src)
            {
                var enumerable = srcType.GetInterfaces()
                        .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));

                if(enumerable!=null)
                {
                    var collection = targetType.GetInterfaces()
                        .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICollection<>));

                    if(collection!=null)
                    {
                        var call = Expression.Call(typeof(TypeConverter<TSource, TTarget>), "CopyCollection", new[] { enumerable.GetGenericArguments()[0], targetType, collection.GetGenericArguments()[0] }, src);
                        return AddNullCheck(srcType, targetType, src, call);
                    }
                }

                return null;

            }

            /// <summary>
            /// Copies the collection.
            /// </summary>
            /// <typeparam name="TSource">The type of the source.</typeparam>
            /// <typeparam name="TCollection">The type of the collection.</typeparam>
            /// <typeparam name="TCollectionElement">The type of the target.</typeparam>
            /// <param name="source">The source.</param>
            /// <returns></returns>
            private static TCollection CopyCollection<TSource, TCollection, TCollectionElement>(IEnumerable<TSource> source) where TCollection : ICollection<TCollectionElement>, new()
            {
                var result = new TCollection();
                foreach (TSource elem in source)
                    result.Add(Converter.ChangeType<TSource, TCollectionElement>(elem));

                return result;
            }

            /// <summary>
            ///   Gets the copy constructor conversion expression, where conversion is attempted by feeding the source to a constructor of the target type.
            /// </summary>
            /// <param name="srcType"> Type of the source. </param>
            /// <param name="targetType"> Type of the target. </param>
            /// <param name="src"> The source. </param>
            /// <returns> </returns>
            private static Expression GetCopyConstructorConversion(Type srcType, Type targetType,
                                                                   ParameterExpression src)
            {
                //first try if constructor exists that excepts our src directly
                var constructor = targetType.GetConstructor(new[] { srcType });

                if (constructor == null)
                {
                    //start looking for a constructor that could accept our src
                    var constructors = targetType.GetConstructors();
                    foreach (var cnstr in constructors)
                    {
                        //get the constructor parameters
                        var parameters = cnstr.GetParameters();
                        if (parameters.Length != 1)
                            continue;

                        //check if it can accept our src
                        if (parameters[0].ParameterType.IsAssignableFrom(srcType))
                        {
                            constructor = cnstr;
                            break;
                        }
                    }
                }

                //check if we found our constructor. If so, use it
                return constructor != null
                           ? AddNullCheck(srcType, targetType, src, Expression.New(constructor, src))
                           : null;
            }

            /// <summary>
            ///   Gets the automatic string conversion.
            /// </summary>
            /// <param name="srcType"> Type of the source. </param>
            /// <param name="targetType"> Type of the target. </param>
            /// <param name="src"> The source. </param>
            /// <returns> </returns>
            private static Expression GetToStringConversion(Type srcType, Type targetType, ParameterExpression src)
            {
                if (targetType == typeof(string))
                {
                    MethodInfo method = srcType.GetMethod("ToString", Type.EmptyTypes);
                    return AddNullCheck(srcType, targetType, src, Expression.Call(src, method));
                }

                return null;
            }


            /// <summary>
            ///   Gets the invalid conversion expression, which throws a InvalidCast exception
            /// </summary>
            /// <param name="srcType"> Type of the source. </param>
            /// <param name="targetType"> Type of the target. </param>
            /// <returns> </returns>
            private static Expression GetInvalidConversion(Type srcType, Type targetType)
            {
                return Expression.Throw(
                    Expression.Constant(
                        new InvalidCastException("Can't convert type " + srcType + " into " + targetType)),
                    targetType);
            }

            /// <summary>
            ///   Adds a null check on the source when applicable.
            /// </summary>
            /// <param name="srcType"> Type of the source. </param>
            /// <param name="targetType"> Type of the target. </param>
            /// <param name="src"> The source. </param>
            /// <param name="call"> The call. </param>
            /// <returns> </returns>
            private static Expression AddNullCheck(Type srcType, Type targetType, Expression src, Expression call)
            {
                //check if src can be null, and if not, return immediatly
                if (srcType.IsValueType)
                    return call;

                return Expression.Condition(
                    Expression.Equal(src, Expression.Constant(null, srcType)), //check for null value

                    //in case of null, return null if target is a reference or nullable type, or throw null reference exception otherwise
                    IsNullableType(targetType)
                        ? (Expression)Expression.Constant(null, targetType)
                        : Expression.Throw(
                            Expression.Constant(
                                new ArgumentNullException("Source may not be null when converting to " +
                                                          targetType)), targetType),
                    //return converted value otherwise
                    call);
            }

            /// <summary>
            ///   Determines whether the specified type can be assigned a null value.
            /// </summary>
            /// <param name="type"> The type. </param>
            /// <returns> </returns>
            private static bool IsNullableType(Type type)
            {
                return (!type.IsValueType) ||
                       (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>));
            }
        }

        #endregion
    }
}