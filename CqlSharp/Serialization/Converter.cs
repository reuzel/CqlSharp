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
using CqlSharp.Annotations;

namespace CqlSharp.Serialization
{
    /// <summary>
    /// Provides a generic conversion routine between types.
    /// </summary>
    public static class Converter
    {
        /// <summary>
        /// The conversion methods provided by the IConvertible interface. Used by the different TypeConverter classes
        /// </summary>
        private static readonly Dictionary<Type, MethodInfo> ConversionMethods =
            typeof(IConvertible).GetMethods()
                                .Where(m => m.Name.StartsWith("To") && m.Name != "ToType")
                                .ToDictionary(m => m.ReturnType, m => m);

        /// <summary>
        /// The conversion functions, mapping the untyped interface to calls on the typed generic conversion interface
        /// </summary>
        private static readonly ConcurrentDictionary<Tuple<Type, Type>, Func<object, object>> ConversionFunctions =
            new ConcurrentDictionary<Tuple<Type, Type>, Func<object, object>>();

        /// <summary>
        /// Changes the type. Uses IConvertible methods when source implements them
        /// </summary>
        /// <typeparam name="TS"> The type of the struct or class to be converted/casted </typeparam>
        /// <typeparam name="TT"> The type of struct or class to which the source must be converted or casted </typeparam>
        /// <param name="source"> The object or value to be casted </param>
        /// <returns> an object or value type to which the source was converted </returns>
        public static TT ChangeType<TS, TT>(TS source)
        {
            return TypeConverter<TS, TT>.Convert(source);
        }

        /// <summary>
        /// Changes the type of the given object to the specific type.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="targetType">Type of the target.</param>
        /// <returns>an object of the given targetType</returns>
        public static object ChangeType(object source, Type targetType)
        {
            var types = Tuple.Create(source.GetType(), targetType);

            var converter = ConversionFunctions.GetOrAdd(types, ts =>
            {
                var src = Expression.Parameter(typeof(object));
                var converted = Expression.Convert(src, ts.Item1);
                var call = Expression.Call(typeof(Converter), "ChangeType", new[] {ts.Item1, ts.Item2}, converted);
                var result = Expression.Convert(call, typeof(object));
                var lambda = Expression.Lambda<Func<object, object>>(result,
                                                                     string.Format("ChangeType<{0},{1}>", ts.Item1.Name,
                                                                                   ts.Item2.Name), new[] {src});
                return lambda.Compile();
            });

            return converter(source);
        }

        #region Nested type: TypeConverter

        /// <summary>
        /// Container class for the conversion routine between two specific types
        /// </summary>
        /// <typeparam name="TSource"> The type of the source. </typeparam>
        /// <typeparam name="TTarget"> The type of the target. </typeparam>
        private static class TypeConverter<TSource, TTarget>
        {
            /// <summary>
            /// The conversion function.
            /// </summary>
            public static readonly Func<TSource, TTarget> Convert;

            /// <summary>
            /// Initializes the <see cref="TypeConverter{TSource, TTarget}" /> conversion routine.
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
                                  GetITypeConverterConversion(srcType, targetType, src) ??
                                  GetDictionaryConversion(srcType, targetType, src) ??
                                  GetCollectionConversion(srcType, targetType, src) ??
                                  GetTupleConversion(srcType, targetType, src) ??
                                  GetToStringConversion(srcType, targetType, src) ??
                                  GetInvalidConversion(srcType, targetType);

                //translate the call into a lamda and compile it
                var lambda = Expression.Lambda<Func<TSource, TTarget>>(call,
                                                                       "Convert" + srcType.Name + "To" + targetType.Name,
                                                                       new[] {src});

                Debug.WriteLine(lambda.Name + " : " + lambda);

                Convert = lambda.Compile();
            }

            /// <summary>
            /// Gets the identity conversion, where src is returned when src and target types are identical
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
            /// Gets the cast conversion expression, where conversion occurs by a direct checked cast between the types
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
            /// Gets a conversion expression using the IConvirtable implementation of the source (if any).
            /// </summary>
            /// <param name="srcType"> Type of the source. </param>
            /// <param name="targetType"> Type of the target. </param>
            /// <param name="src"> The source. </param>
            /// <returns> </returns>
            private static Expression GetIConvertibleConversion(Type srcType, Type targetType, ParameterExpression src)
            {
                Expression call = null;

                //Check if type implements IConvertible
                if(typeof(IConvertible).IsAssignableFrom(srcType))
                {
                    //remove any nullable wrappers on the target, increasing the chance of finding the right conversion method
                    var nonNullableTargetType = Nullable.GetUnderlyingType(targetType) ?? targetType;

                    //check if one of the conversion methods can be used
                    MethodInfo method;
                    if(ConversionMethods.TryGetValue(nonNullableTargetType, out method))
                    {
                        // ReSharper disable once PossiblyMistakenUseOfParamsMethod
                        call = Expression.Call(src, method, Expression.Constant(null, typeof(IFormatProvider)));

                        //convert result back to nullable value if necessary
                        if(nonNullableTargetType != targetType)
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

            private static Expression GetITypeConverterConversion(Type srcType, Type targetType, ParameterExpression src)
            {
                //check if src has a related TypeConverter attribute
                var converterAttribute =
                    Attribute.GetCustomAttribute(srcType, typeof(CqlTypeConverterAttribute)) as
                        CqlTypeConverterAttribute;

                if(converterAttribute != null)
                {
                    var converterType = converterAttribute.Converter;
                    var converter = Expression.Constant(Activator.CreateInstance(converterType));
                    var call = Expression.Call(converter, "ConvertTo", new[] {targetType}, src);
                    return AddNullCheck(srcType, targetType, src, call);
                }

                //check if target has a related TypeConverter attribute
                converterAttribute =
                    Attribute.GetCustomAttribute(targetType, typeof(CqlTypeConverterAttribute)) as
                        CqlTypeConverterAttribute;

                if(converterAttribute != null)
                {
                    var converterType = converterAttribute.Converter;
                    var converter = Expression.Constant(Activator.CreateInstance(converterType));
                    var call = Expression.Call(converter, "ConvertFrom", new[] {srcType}, src);
                    return AddNullCheck(srcType, targetType, src, call);
                }

                return null;
            }

            private static Expression GetTupleConversion(Type srcType, Type targetType, ParameterExpression src)
            {
                //tuple types are generic types
                if(!srcType.IsGenericType && !targetType.IsGenericType)
                    return null;

                //are they tuple types?
                if(!TypeExtensions.TupleTypes.Contains(srcType.GetGenericTypeDefinition()) ||
                   !TypeExtensions.TupleTypes.Contains(targetType.GetGenericTypeDefinition()))
                    return null;

                var srcArguments = srcType.GetGenericArguments();
                var targetArguments = targetType.GetGenericArguments();

                //conversion only possible if target has equal or smaller number of fields
                if(targetArguments.Length > srcArguments.Length)
                    return null;

                //iterate over all items and convert the values
                var expressions = new Expression[targetArguments.Length];
                for(int i = 0; i < targetArguments.Length; i++)
                {
                    var sourceMember = srcType.GetProperty("Item" + (i + 1));

                    var value = Expression.Property(src, sourceMember);
                    expressions[i] = Expression.Call(typeof(Converter),
                                                     "ChangeType",
                                                     new[] {value.Type, targetArguments[i]},
                                                     value);
                }

                //create the new tupe
                var newTuple = Expression.Call(typeof(Tuple),
                                               "Create",
                                               expressions.Select(e => e.Type).ToArray(),
                                               expressions);

                //return the new tuple
                return AddNullCheck(srcType, targetType, src, newTuple);
            }


            private static Expression GetCollectionConversion(Type srcType, Type targetType, ParameterExpression src)
            {
                var enumerable = srcType.GetInterfaces()
                                        .FirstOrDefault(
                                            i =>
                                                i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));

                if(enumerable != null)
                {
                    var collection = targetType.GetInterfaces()
                                               .FirstOrDefault(
                                                   i =>
                                                       i.IsGenericType &&
                                                       i.GetGenericTypeDefinition() == typeof(ICollection<>));

                    if(collection != null)
                    {
                        var call = Expression.Call(typeof(TypeConverter<TSource, TTarget>), "CopyCollection",
                                                   new[]
                                                   {
                                                       enumerable.GetGenericArguments()[0], targetType,
                                                       collection.GetGenericArguments()[0]
                                                   }, src);
                        return AddNullCheck(srcType, targetType, src, call);
                    }
                }

                return null;
            }

            /// <summary>
            /// Copies the collection.
            /// </summary>
            /// <typeparam name="TSourceElement">The type of the source.</typeparam>
            /// <typeparam name="TCollection">The type of the collection.</typeparam>
            /// <typeparam name="TCollectionElement">The type of the target.</typeparam>
            /// <param name="source">The source.</param>
            /// <returns></returns>
            [UsedImplicitly]
            private static TCollection CopyCollection<TSourceElement, TCollection, TCollectionElement>(
                IEnumerable<TSourceElement> source) where TCollection : ICollection<TCollectionElement>, new()
            {
                var result = new TCollection();
                foreach(TSourceElement elem in source)
                    result.Add(ChangeType<TSourceElement, TCollectionElement>(elem));

                return result;
            }

            /// <summary>
            /// Gets the dictionary conversion.
            /// </summary>
            /// <param name="srcType">Type of the source.</param>
            /// <param name="targetType">Type of the target.</param>
            /// <param name="src">The source.</param>
            /// <returns></returns>
            private static Expression GetDictionaryConversion(Type srcType, Type targetType, ParameterExpression src)
            {
                var srcDictionary = srcType.GetInterfaces()
                                           .FirstOrDefault(
                                               i =>
                                                   i.IsGenericType &&
                                                   i.GetGenericTypeDefinition() == typeof(IDictionary<,>));

                if(srcDictionary != null)
                {
                    var targetDictionary = targetType.GetInterfaces()
                                                     .FirstOrDefault(
                                                         i =>
                                                             i.IsGenericType &&
                                                             i.GetGenericTypeDefinition() == typeof(IDictionary<,>));

                    if(targetDictionary != null)
                    {
                        var call = Expression.Call(typeof(TypeConverter<TSource, TTarget>), "CopyDictionary",
                                                   new[]
                                                   {
                                                       srcType,
                                                       srcDictionary.GetGenericArguments()[0],
                                                       srcDictionary.GetGenericArguments()[1],
                                                       targetType,
                                                       targetDictionary.GetGenericArguments()[0],
                                                       targetDictionary.GetGenericArguments()[1]
                                                   }, src);
                        return AddNullCheck(srcType, targetType, src, call);
                    }
                }

                return null;
            }

            /// <summary>
            /// Copies the dictionary into a dictionary of a new type.
            /// </summary>
            /// <typeparam name="TSourceD">The type of the source dictionary.</typeparam>
            /// <typeparam name="TSourceKey">The type of the source key.</typeparam>
            /// <typeparam name="TSourceVal">The type of the source value.</typeparam>
            /// <typeparam name="TTargetD">The type of the target dictionary.</typeparam>
            /// <typeparam name="TTargetKey">The type of the target key.</typeparam>
            /// <typeparam name="TTargetVal">The type of the target value.</typeparam>
            /// <param name="source">The source.</param>
            /// <returns></returns>
            [UsedImplicitly]
            private static TTargetD CopyDictionary<TSourceD, TSourceKey, TSourceVal, TTargetD, TTargetKey, TTargetVal>(
                TSourceD source)
                where TSourceD : IDictionary<TSourceKey, TSourceVal>
                where TTargetD : IDictionary<TTargetKey, TTargetVal>, new()
            {
                var result = new TTargetD();
                foreach(var kvp in source)
                {
                    result.Add(ChangeType<TSourceKey, TTargetKey>(kvp.Key),
                               ChangeType<TSourceVal, TTargetVal>(kvp.Value));
                }

                return result;
            }


            /// <summary>
            /// Gets the automatic string conversion.
            /// </summary>
            /// <param name="srcType"> Type of the source. </param>
            /// <param name="targetType"> Type of the target. </param>
            /// <param name="src"> The source. </param>
            /// <returns> </returns>
            private static Expression GetToStringConversion(Type srcType, Type targetType, ParameterExpression src)
            {
                if(targetType == typeof(string))
                {
                    MethodInfo method = srcType.GetMethod("ToString", Type.EmptyTypes);
                    return AddNullCheck(srcType, targetType, src, Expression.Call(src, method));
                }

                return null;
            }


            /// <summary>
            /// Gets the invalid conversion expression, which throws a InvalidCast exception
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
            /// Adds a null check on the source when applicable.
            /// </summary>
            /// <param name="srcType"> Type of the source. </param>
            /// <param name="targetType"> Type of the target. </param>
            /// <param name="src"> The source. </param>
            /// <param name="call"> The call. </param>
            /// <returns> </returns>
            private static Expression AddNullCheck(Type srcType, Type targetType, Expression src, Expression call)
            {
                //check if src can be null, and if not, return immediatly
                if(srcType.IsValueType)
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
            /// Determines whether the specified type can be assigned a null value.
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