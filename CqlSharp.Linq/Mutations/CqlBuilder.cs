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

using CqlSharp.Serialization;
using System;
using System.Linq;
using System.Text;

namespace CqlSharp.Linq.Mutations
{
    internal static class CqlBuilder<TEntity> where TEntity : class, new()
    {
        private static readonly ObjectAccessor<TEntity> Accessor = ObjectAccessor<TEntity>.Instance;

        /// <summary>
        ///   Gets the reload CQL statement.
        /// </summary>
        /// <returns> </returns>
        public static string GetSelectQuery(CqlTable<TEntity> table, EntityKey<TEntity> key)
        {
            var accessor = ObjectAccessor<TEntity>.Instance;

            var sb = new StringBuilder();
            sb.Append("SELECT ");

            bool firstColumn = true;
            foreach (var column in accessor.Columns)
            {
                if (!firstColumn)
                {
                    sb.Append(",");
                }
                sb.Append(" \"");
                sb.Append(column.Name);
                sb.Append("\"");
                firstColumn = false;
            }
            sb.Append(" FROM \"");
            sb.Append(table.Name);
            sb.Append("\" WHERE");

            firstColumn = true;
            foreach (var keyColumn in accessor.PartitionKeys.Concat(accessor.ClusteringKeys))
            {
                if (!firstColumn)
                    sb.Append(" AND ");
                sb.Append(" \"");
                sb.Append(keyColumn.Name);
                sb.Append("\"=");
                var value = keyColumn.Read<object>(key.Values);
                sb.Append(TypeSystem.ToStringValue(value, keyColumn.CqlType));
                firstColumn = false;
            }

            return sb.ToString();
        }

        /// <summary>
        ///   Builds the DML query.
        /// </summary>
        /// <param name="item"> The tracked item. </param>
        /// <returns> </returns>
        /// <exception cref="System.InvalidOperationException"></exception>
        /// <exception cref="System.NotImplementedException">InsertOrUpdate is not yet implemented
        ///   or
        ///   PossibleUpdate is not yet implemented</exception>
        public static string BuildDmlQuery(EntityEntry<TEntity> item)
        {
            switch (item.State)
            {
                case EntityState.Deleted:
                    return BuildDeleteStatement(item);
                case EntityState.Added:
                    return BuildInsertStatement(item);
                case EntityState.Modified:
                    return BuildUpdateStatement(item);
                case EntityState.Unchanged:
                    return string.Empty;
                default:
                    throw new InvalidOperationException();
            }
        }

        #region Delete functions

        private static string BuildDeleteStatement(EntityEntry<TEntity> item)
        {
            var deleteSb = new StringBuilder();
            deleteSb.Append("DELETE FROM \"");
            deleteSb.Append(item.Table.Name.Replace("\"", "\"\"").Replace(".", "\".\""));
            deleteSb.Append("\" WHERE ");
            TranslatePrimaryConditions(deleteSb, item);
            deleteSb.Append(";");

            return deleteSb.ToString();
        }

        #endregion

        #region Update functions

        private static string BuildUpdateStatement(EntityEntry<TEntity> item)
        {
            var updateSb = new StringBuilder();
            updateSb.Append("UPDATE \"");
            updateSb.Append(item.Table.Name.Replace("\"", "\"\"").Replace(".", "\".\""));
            updateSb.Append("\" SET ");
            TranslateUpdationIdValPairs(updateSb, item);
            updateSb.Append(" WHERE ");
            TranslatePrimaryConditions(updateSb, item);
            updateSb.Append(";");

            return updateSb.ToString();
        }

        private static void TranslateUpdationIdValPairs(StringBuilder builder, EntityEntry<TEntity> entityEntry)
        {
            bool first = true;
            foreach (ICqlColumnInfo<TEntity> column in entityEntry.ChangedColumns)
            {
                if (!first)
                    builder.Append(", ");

                builder.Append("\"");
                builder.Append(column.Name.Replace("\"", "\"\""));
                builder.Append("\"=");

                var value = column.Read<object>(entityEntry.Entity);
                builder.Append(TypeSystem.ToStringValue(value, column.CqlType));

                first = false;
            }
        }

        private static void TranslatePrimaryConditions(StringBuilder builder, EntityEntry<TEntity> entityEntry)
        {
            bool first = true;
            foreach (var keyColumn in Accessor.PartitionKeys.Concat(Accessor.ClusteringKeys))
            {
                if (!first)
                    builder.Append(" AND ");

                builder.Append("\"");
                builder.Append(keyColumn.Name.Replace("\"", "\"\""));
                builder.Append("\"=");
                var value = keyColumn.Read<object>(entityEntry.Entity);
                builder.Append(TypeSystem.ToStringValue(value, keyColumn.CqlType));

                first = false;
            }
        }

        #endregion

        #region Insert functions

        private static string BuildInsertStatement(EntityEntry<TEntity> item)
        {
            var insertSb = new StringBuilder();
            insertSb.Append("INSERT INTO \"");
            insertSb.Append(item.Table.Name.Replace("\"", "\"\"").Replace(".", "\".\""));
            insertSb.Append("\" (");
            TranslateInsertionIds(insertSb, item);
            insertSb.Append(")");
            insertSb.Append(" VALUES ");
            insertSb.Append("(");
            TranslateInsertionValues(insertSb, item);
            insertSb.Append(");");

            return insertSb.ToString();
        }

        private static void TranslateInsertionIds(StringBuilder builder, EntityEntry<TEntity> entityEntry)
        {
            bool first = true;
            foreach (var column in Accessor.Columns)
            {
                //skip null values
                if (column.Read<object>(entityEntry.Entity) == null)
                    continue;

                if (!first)
                    builder.Append(", ");

                builder.Append("\"");
                builder.Append(column.Name.Replace("\"", "\"\""));
                builder.Append("\"");

                first = false;
            }
        }

        private static void TranslateInsertionValues(StringBuilder builder, EntityEntry<TEntity> entityEntry)
        {
            bool first = true;
            foreach (var column in Accessor.Columns)
            {
                //skip null values
                var value = column.Read<object>(entityEntry.Entity);
                if (value == null)
                    continue;

                //add ',' if not first
                if (!first)
                    builder.Append(", ");

                //write value
                builder.Append(TypeSystem.ToStringValue(value, column.CqlType));

                first = false;
            }
        }

        #endregion
    }
}