// CqlSharp.Linq - CqlSharp.Linq
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
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace CqlSharp.Linq.Query
{
    public static class CompiledQuery
    {
        private static readonly MethodInfo ExecuteMethod = typeof(IQueryPlan).GetMethod("Execute");
        
        private static Delegate BuildCompiledQuery<TResult>(Expression query)
        {
            var lambda = (LambdaExpression)query.StripQuotes();

            //create a query plan
            var provider = new CqlQueryProvider();
            var plan = Expression.Constant(provider.CreateQueryPlan(lambda));

            //get the context (always the first parameter)
            var context = lambda.Parameters[0];

            //map all the remaining parameters to an array of objects
            var parameters = lambda.Parameters.Skip(1).Select(prm => Expression.Convert(prm, typeof(object)));
            var parameterArray = Expression.NewArrayInit(typeof(object), parameters);

            //get correct executeMethod
            var method = ExecuteMethod.MakeGenericMethod(typeof (TResult));

            //call QueryPlan.Execute with the given parameter set
            var executeCall = Expression.Call(plan, method, context, parameterArray);
            
            //create and return the compiled query delegate
            var compiledQuery = Expression.Lambda(executeCall, lambda.Parameters);
            return compiledQuery.Compile();
        }

        /// <summary>
        /// Compiles the specified query.
        /// </summary>
        /// <typeparam name="TContext">The type of the context.</typeparam>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public static Func<TContext, TResult> Compile<TContext, TResult>(Expression<Func<TContext, TResult>> query)
            where TContext : CqlContext
        {
            return (Func<TContext, TResult>)BuildCompiledQuery<TResult>(query);
        }

        /// <summary>
        /// Compiles the specified query.
        /// </summary>
        /// <typeparam name="TContext">The type of the context.</typeparam>
        /// <typeparam name="TArg1">The type of the arg1.</typeparam>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public static Func<TContext, TArg1, TResult> Compile<TContext, TArg1, TResult>(Expression<Func<TContext, TArg1, TResult>> query)
            where TContext : CqlContext
        {
            return (Func<TContext, TArg1, TResult>)BuildCompiledQuery<TResult>(query);
        }

        /// <summary>
        /// Compiles the specified query.
        /// </summary>
        /// <typeparam name="TContext">The type of the context.</typeparam>
        /// <typeparam name="TArg1">The type of the arg1.</typeparam>
        /// <typeparam name="TArg2">The type of the arg2.</typeparam>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public static Func<TContext, TArg1, TArg2, TResult> Compile<TContext, TArg1, TArg2, TResult>(Expression<Func<TContext, TArg1, TArg2, TResult>> query)
           where TContext : CqlContext
        {
            return (Func<TContext, TArg1, TArg2, TResult>)BuildCompiledQuery<TResult>(query);
        }

        /// <summary>
        /// Compiles the specified query.
        /// </summary>
        /// <typeparam name="TContext">The type of the context.</typeparam>
        /// <typeparam name="TArg1">The type of the arg1.</typeparam>
        /// <typeparam name="TArg2">The type of the arg2.</typeparam>
        /// <typeparam name="TArg3">The type of the arg3.</typeparam>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public static Func<TContext, TArg1, TArg2, TArg3, TResult> Compile<TContext, TArg1, TArg2, TArg3, TResult>(Expression<Func<TContext, TArg1, TArg2, TArg3, TResult>> query)
          where TContext : CqlContext
        {
            return (Func<TContext, TArg1, TArg2, TArg3, TResult>)BuildCompiledQuery<TResult>(query);
        }

        /// <summary>
        /// Compiles the specified query.
        /// </summary>
        /// <typeparam name="TContext">The type of the context.</typeparam>
        /// <typeparam name="TArg1">The type of the arg1.</typeparam>
        /// <typeparam name="TArg2">The type of the arg2.</typeparam>
        /// <typeparam name="TArg3">The type of the arg3.</typeparam>
        /// <typeparam name="TArg4">The type of the arg4.</typeparam>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public static Func<TContext, TArg1, TArg2, TArg3, TArg4, TResult> Compile<TContext, TArg1, TArg2, TArg3, TArg4, TResult>(Expression<Func<TContext, TArg1, TArg2, TArg3, TArg4, TResult>> query)
         where TContext : CqlContext
        {
            return (Func<TContext, TArg1, TArg2, TArg3, TArg4, TResult>)BuildCompiledQuery<TResult>(query);
        }

        /// <summary>
        /// Compiles the specified query.
        /// </summary>
        /// <typeparam name="TContext">The type of the context.</typeparam>
        /// <typeparam name="TArg1">The type of the arg1.</typeparam>
        /// <typeparam name="TArg2">The type of the arg2.</typeparam>
        /// <typeparam name="TArg3">The type of the arg3.</typeparam>
        /// <typeparam name="TArg4">The type of the arg4.</typeparam>
        /// <typeparam name="TArg5">The type of the arg5.</typeparam>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public static Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TResult> Compile<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TResult>(Expression<Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TResult>> query)
        where TContext : CqlContext
        {
            return (Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TResult>)BuildCompiledQuery<TResult>(query);
        }

        /// <summary>
        /// Compiles the specified query.
        /// </summary>
        /// <typeparam name="TContext">The type of the context.</typeparam>
        /// <typeparam name="TArg1">The type of the arg1.</typeparam>
        /// <typeparam name="TArg2">The type of the arg2.</typeparam>
        /// <typeparam name="TArg3">The type of the arg3.</typeparam>
        /// <typeparam name="TArg4">The type of the arg4.</typeparam>
        /// <typeparam name="TArg5">The type of the arg5.</typeparam>
        /// <typeparam name="TArg6">The type of the arg6.</typeparam>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public static Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TResult> Compile<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TResult>(Expression<Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TResult>> query)
        where TContext : CqlContext
        {
            return (Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TResult>)BuildCompiledQuery<TResult>(query);
        }

        /// <summary>
        /// Compiles the specified query.
        /// </summary>
        /// <typeparam name="TContext">The type of the context.</typeparam>
        /// <typeparam name="TArg1">The type of the arg1.</typeparam>
        /// <typeparam name="TArg2">The type of the arg2.</typeparam>
        /// <typeparam name="TArg3">The type of the arg3.</typeparam>
        /// <typeparam name="TArg4">The type of the arg4.</typeparam>
        /// <typeparam name="TArg5">The type of the arg5.</typeparam>
        /// <typeparam name="TArg6">The type of the arg6.</typeparam>
        /// <typeparam name="TArg7">The type of the arg7.</typeparam>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public static Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TArg7, TResult> Compile<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TArg7, TResult>(Expression<Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TArg7, TResult>> query)
        where TContext : CqlContext
        {
            return (Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TArg7, TResult>)BuildCompiledQuery<TResult>(query);
        }

        /// <summary>
        /// Compiles the specified query.
        /// </summary>
        /// <typeparam name="TContext">The type of the context.</typeparam>
        /// <typeparam name="TArg1">The type of the arg1.</typeparam>
        /// <typeparam name="TArg2">The type of the arg2.</typeparam>
        /// <typeparam name="TArg3">The type of the arg3.</typeparam>
        /// <typeparam name="TArg4">The type of the arg4.</typeparam>
        /// <typeparam name="TArg5">The type of the arg5.</typeparam>
        /// <typeparam name="TArg6">The type of the arg6.</typeparam>
        /// <typeparam name="TArg7">The type of the arg7.</typeparam>
        /// <typeparam name="TArg8">The type of the arg8.</typeparam>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public static Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TArg7, TArg8, TResult> Compile<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TArg7, TArg8, TResult>(Expression<Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TArg7, TArg8, TResult>> query)
        where TContext : CqlContext
        {
            return (Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TArg7, TArg8, TResult>)BuildCompiledQuery<TResult>(query);
        }

        /// <summary>
        /// Compiles the specified query.
        /// </summary>
        /// <typeparam name="TContext">The type of the context.</typeparam>
        /// <typeparam name="TArg1">The type of the arg1.</typeparam>
        /// <typeparam name="TArg2">The type of the arg2.</typeparam>
        /// <typeparam name="TArg3">The type of the arg3.</typeparam>
        /// <typeparam name="TArg4">The type of the arg4.</typeparam>
        /// <typeparam name="TArg5">The type of the arg5.</typeparam>
        /// <typeparam name="TArg6">The type of the arg6.</typeparam>
        /// <typeparam name="TArg7">The type of the arg7.</typeparam>
        /// <typeparam name="TArg8">The type of the arg8.</typeparam>
        /// <typeparam name="TArg9">The type of the arg9.</typeparam>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public static Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TArg7, TArg8, TArg9, TResult> Compile<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TArg7, TArg8, TArg9, TResult>(Expression<Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TArg7, TArg8, TArg9, TResult>> query)
        where TContext : CqlContext
        {
            return (Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TArg7, TArg8, TArg9, TResult>)BuildCompiledQuery<TResult>(query);
        }

        /// <summary>
        /// Compiles the specified query.
        /// </summary>
        /// <typeparam name="TContext">The type of the context.</typeparam>
        /// <typeparam name="TArg1">The type of the arg1.</typeparam>
        /// <typeparam name="TArg2">The type of the arg2.</typeparam>
        /// <typeparam name="TArg3">The type of the arg3.</typeparam>
        /// <typeparam name="TArg4">The type of the arg4.</typeparam>
        /// <typeparam name="TArg5">The type of the arg5.</typeparam>
        /// <typeparam name="TArg6">The type of the arg6.</typeparam>
        /// <typeparam name="TArg7">The type of the arg7.</typeparam>
        /// <typeparam name="TArg8">The type of the arg8.</typeparam>
        /// <typeparam name="TArg9">The type of the arg9.</typeparam>
        /// <typeparam name="TArg10">The type of the arg10.</typeparam>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public static Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TArg7, TArg8, TArg9, TArg10, TResult> Compile<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TArg7, TArg8, TArg9, TArg10, TResult>(Expression<Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TArg7, TArg8, TArg9, TArg10, TResult>> query)
        where TContext : CqlContext
        {
            return (Func<TContext, TArg1, TArg2, TArg3, TArg4, TArg5, TArg6, TArg7, TArg8, TArg9, TArg10, TResult>)BuildCompiledQuery<TResult>(query);
        }
    }
}