// CqlSharp - CqlSharp
// Copyright (c) 2013 Joost Reuzel
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

using CqlSharp.Linq.Expressions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Numerics;
using System.Text;

namespace CqlSharp.Linq
{
    /// <summary>
    /// Distills CQL Select Statement query strings from an expression tree
    /// </summary>
    internal class CqlTextBuilder : CqlExpressionVisitor
    {
        private readonly Dictionary<Expression, string> _translations = new Dictionary<Expression, string>();
        private readonly List<SelectStatementExpression> _selects = new List<SelectStatementExpression>();

        /// <summary>
        /// Builds selectStatement queries from the specified expression, and returns the first instance
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns></returns>
        public string Build(Expression expression)
        {
            Visit(expression);
            return _translations[_selects.First()];
        }

        /// <summary>
        /// Gets the selectStatement queries.
        /// </summary>
        /// <value>
        /// The selectStatement queries.
        /// </value>
        public IEnumerable<string> SelectQueries
        {
            get { return _translations.Where(t => _selects.Contains(t.Key)).Select(t => t.Value); }
        }

        public override Expression VisitSelectStatement(SelectStatementExpression selectStatement)
        {
            base.VisitSelectStatement(selectStatement);

            var builder = new StringBuilder();
            builder.Append("SELECT ");
            builder.Append(_translations[selectStatement.SelectClause]);
            builder.Append(" FROM ");
            builder.Append(selectStatement.TableName);

            if (selectStatement.WhereClause != null && selectStatement.WhereClause.Any())
            {
                builder.Append(" WHERE ");
                var wheres = selectStatement.WhereClause.Select(relation => _translations[relation]);
                builder.Append(string.Join(" AND ", wheres));
            }

            if (selectStatement.OrderBy != null && selectStatement.OrderBy.Any())
            {
                builder.Append(" ORDER BY ");
                var orders = selectStatement.OrderBy.Select(order => _translations[order]);
                builder.Append(string.Join(",", orders));
            }

            if (selectStatement.Limit.HasValue)
            {
                builder.Append(" LIMIT ");
                builder.Append(selectStatement.Limit);
            }

            builder.Append(";");

            _translations[selectStatement] = builder.ToString();
            _selects.Add(selectStatement);

            return selectStatement;
        }

        public override Expression VisitSelectClause(SelectClauseExpression selectClauseExpression)
        {
            base.VisitSelectClause(selectClauseExpression);

            string translation;
            switch ((CqlExpressionType)selectClauseExpression.NodeType)
            {
                case CqlExpressionType.SelectAll:
                    translation = "*";
                    break;
                case CqlExpressionType.SelectCount:
                    translation = "COUNT(*)";
                    break;
                case CqlExpressionType.SelectColumns:
                    var selectors = selectClauseExpression.Selectors.Select(arg => _translations[arg]);
                    translation = string.Join(",", selectors);
                    break;
                default:
                    throw new Exception("Unexpected type of select clause encountered : " + selectClauseExpression.NodeType);
            }

            _translations[selectClauseExpression] = translation;
            return selectClauseExpression;
        }

        public override Expression VisitSelector(SelectorExpression selector)
        {
            base.VisitSelector(selector);

            string value;

            switch ((CqlExpressionType)selector.NodeType)
            {
                case CqlExpressionType.IdentifierSelector:
                    value = _translations[selector.Identifier];
                    break;
                case CqlExpressionType.TtlSelector:
                    value = string.Format("TTL({0})", _translations[selector.Identifier]);
                    break;
                case CqlExpressionType.WriteTimeSelector:
                    value = string.Format("WRITETIME({0})", _translations[selector.Identifier]);
                    break;
                case CqlExpressionType.FunctionSelector:
                    var builder = new StringBuilder();
                    builder.Append(selector.Function.ToString());
                    builder.Append("(");
                    var argsAsString = selector.Arguments.Select(arg => _translations[arg]);
                    builder.Append(string.Join(",", argsAsString));
                    builder.Append(")");
                    value = builder.ToString();
                    break;
                default:
                    throw new CqlLinqException("Unexpected Selector type encountered");
            }

            _translations[selector] = value;

            return selector;
        }

        public override Expression VisitRelation(RelationExpression relation)
        {
            base.VisitRelation(relation);

            var builder = new StringBuilder();

            switch ((CqlExpressionType)relation.NodeType)
            {
                case CqlExpressionType.Equal:
                    builder.Append(_translations[relation.Identifiers.Single()]);
                    builder.Append("=");
                    builder.Append(_translations[relation.Terms.Single()]);
                    break;
                case CqlExpressionType.LargerEqualThan:
                    builder.Append(_translations[relation.Identifiers.Single()]);
                    builder.Append(">=");
                    builder.Append(_translations[relation.Terms.Single()]);
                    break;
                case CqlExpressionType.LargerThan:
                    builder.Append(_translations[relation.Identifiers.Single()]);
                    builder.Append(">");
                    builder.Append(_translations[relation.Terms.Single()]);
                    break;
                case CqlExpressionType.SmallerEqualThan:
                    builder.Append(_translations[relation.Identifiers.Single()]);
                    builder.Append("<=");
                    builder.Append(_translations[relation.Terms.Single()]);
                    break;
                case CqlExpressionType.SmallerThan:
                    builder.Append(_translations[relation.Identifiers.Single()]);
                    builder.Append("<");
                    builder.Append(_translations[relation.Terms.Single()]);
                    break;
                case CqlExpressionType.TokenEqual:
                    builder.Append("token(");
                    builder.Append(string.Join(",", relation.Identifiers.Select(id => _translations[id])));
                    builder.Append(")");
                    builder.Append("=");
                    builder.Append(_translations[relation.Terms.Single()]);
                    break;
                case CqlExpressionType.TokenLargerEqualThan:
                    builder.Append("token(");
                    builder.Append(string.Join(",", relation.Identifiers.Select(id => _translations[id])));
                    builder.Append(")");
                    builder.Append(">=");
                    builder.Append(_translations[relation.Terms.Single()]);
                    break;
                case CqlExpressionType.TokenLargerThan:
                    builder.Append("token(");
                    builder.Append(string.Join(",", relation.Identifiers.Select(id => _translations[id])));
                    builder.Append(")");
                    builder.Append(">");
                    builder.Append(_translations[relation.Terms.Single()]);
                    break;
                case CqlExpressionType.TokenSmallerEqualThan:
                    builder.Append("token(");
                    builder.Append(string.Join(",", relation.Identifiers.Select(id => _translations[id])));
                    builder.Append(")");
                    builder.Append("<=");
                    builder.Append(_translations[relation.Terms.Single()]);
                    break;
                case CqlExpressionType.TokenSmallerThan:
                    builder.Append("token(");
                    builder.Append(string.Join(",", relation.Identifiers.Select(id => _translations[id])));
                    builder.Append(")");
                    builder.Append("<");
                    builder.Append(_translations[relation.Terms.Single()]);
                    break;
                case CqlExpressionType.In:
                    builder.Append(_translations[relation.Identifiers.Single()]);
                    builder.Append(" IN (");
                    builder.Append(string.Join(",", relation.Terms.Select(term => _translations[term])));
                    builder.Append(")");
                    break;
                default:
                    throw new CqlLinqException("Unexpected relation encountered in where: " +
                                               relation.NodeType.ToString());
            }

            _translations[relation] = builder.ToString();

            return relation;
        }

        public override Expression VisitTerm(TermExpression term)
        {
            var builder = new StringBuilder();

            switch ((CqlExpressionType)term.NodeType)
            {
                case CqlExpressionType.Variable:
                    builder.Append("?");
                    break;

                case CqlExpressionType.Constant:
                    builder.Append(ToStringValue(term.Value, term.Type));
                    break;

                case CqlExpressionType.List:
                    {
                        builder.Append("[");
                        Type listType = term.Value.GetType();
                        Type elementType = listType.GetGenericArguments()[0];
                        var elements = new List<string>();
                        foreach (var value in (IEnumerable)term.Value)
                            elements.Add(ToStringValue(value, elementType));
                        builder.Append(string.Join(",", elements));
                        builder.Append("]");
                    }
                    break;

                case CqlExpressionType.Set:
                    {
                        builder.Append("{");
                        Type listType = term.Value.GetType();
                        Type elementType = listType.GetGenericArguments()[0];
                        var elements = new List<string>();
                        foreach (var value in (IEnumerable)term.Value)
                            elements.Add(ToStringValue(value, elementType));
                        builder.Append(string.Join(",", elements));
                        builder.Append("}");
                    }
                    break;

                case CqlExpressionType.Map:
                    {
                        builder.Append("{");
                        Type listType = term.Value.GetType();
                        Type keyType = listType.GetGenericArguments()[0];
                        Type elementType = listType.GetGenericArguments()[1];
                        var dict = (IDictionary)term.Value;

                        var elements = new List<string>();
                        foreach (DictionaryEntry value in dict)
                        {
                            elements.Add(string.Format("{0}:{1}",
                                                       ToStringValue(value.Key, keyType),
                                                       ToStringValue(value.Value, elementType)));
                        }

                        builder.Append(string.Join(",", elements));
                        builder.Append("}");
                    }
                    break;

                case CqlExpressionType.Function:
                    builder.Append(term.Function.ToString());
                    builder.Append("(");
                    builder.Append(string.Join(",", term.Arguments.Select(arg => _translations[arg])));
                    builder.Append(")");
                    break;

                default:
                    throw new CqlLinqException("Unexpected type of term encountered: " + term.NodeType.ToString());
            }

            _translations[term] = builder.ToString();
            return term;
        }

        private string ToStringValue(object value, Type type)
        {
            switch (type.ToCqlType())
            {
                case CqlType.Text:
                case CqlType.Varchar:
                case CqlType.Ascii:
                    var str = (string)value;
                    return "'" + str.Replace("'", "''") + "'";

                case CqlType.Boolean:
                    return ((bool)value) ? "true" : "false";

                case CqlType.Decimal:
                case CqlType.Double:
                case CqlType.Float:
                    var culture = CultureInfo.InvariantCulture;
                    return string.Format(culture, "{0:E}", value);

                case CqlType.Counter:
                case CqlType.Bigint:
                case CqlType.Int:
                    return string.Format("{0:D}", value);

                case CqlType.Timeuuid:
                case CqlType.Uuid:
                    return ((Guid)value).ToString("D");

                case CqlType.Varint:
                    return ((BigInteger)value).ToString("D");

                case CqlType.Timestamp:
                    long timestamp = ((DateTime)value).ToTimestamp();
                    return string.Format("{0:D}", timestamp);

                case CqlType.Blob:
                    return ((byte[])value).ToHex("0x");

                default:
                    throw new CqlLinqException("Unable to translate term to a string representation");
            }
        }

        public override Expression VisitOrdering(OrderingExpression ordering)
        {
            string value;

            switch ((CqlExpressionType)ordering.NodeType)
            {
                case CqlExpressionType.OrderDescending:
                    value = _translations[ordering.Identifier] + " DESC";
                    break;
                case CqlExpressionType.OrderAscending:
                    value = _translations[ordering.Identifier] + " ASC";
                    break;
                default:
                    throw new CqlLinqException("Unexpected ordering type encountered: " + ordering.NodeType.ToString());
            }

            _translations[ordering] = value;

            return ordering;
        }

        public override Expression VisitIdentifier(IdentifierExpression identifier)
        {
            base.VisitIdentifier(identifier);
            _translations[identifier] = identifier.Name;
            return identifier;
        }
    }
}