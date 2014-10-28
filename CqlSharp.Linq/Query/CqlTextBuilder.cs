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

using CqlSharp.Linq.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace CqlSharp.Linq.Query
{
    /// <summary>
    ///   Distills CQL Select Statement query strings from an expression tree
    /// </summary>
    internal class CqlTextBuilder : CqlExpressionVisitor
    {
        private readonly Dictionary<Expression, string> _translations = new Dictionary<Expression, string>();

        /// <summary>
        ///   Builds selectStatement queries from the specified expression, and returns the first instance
        /// </summary>
        /// <param name="expression"> The expression. </param>
        /// <returns> </returns>
        public string Build(Expression expression)
        {
            Expression expr = Visit(expression);
            return _translations[expr];
        }

        public override Expression VisitSelectStatement(SelectStatementExpression selectStatement)
        {
            base.VisitSelectStatement(selectStatement);

            var builder = new StringBuilder();
            builder.Append("SELECT ");
            builder.Append(_translations[selectStatement.SelectClause]);
            builder.Append(" FROM \"");
            builder.Append(selectStatement.TableName.Replace("\"", "\"\"").Replace(".","\".\""));
            builder.Append("\"");

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

            if (selectStatement.AllowFiltering)
            {
                builder.Append(" ALLOW FILTERING");
            }

            builder.Append(";");

            _translations[selectStatement] = builder.ToString();

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
                    translation = selectClauseExpression.Distinct ? "DISTINCT " : "";
                    var selectors = selectClauseExpression.Selectors.Select(arg => _translations[arg]);
                    translation += string.Join(",", selectors);
                    break;
                default:
                    throw new Exception("Unexpected type of select clause encountered : " +
                                        selectClauseExpression.NodeType);
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
                    value = "\"" + selector.Identifier.Replace("\"", "\"\"") + "\"";
                    break;
                case CqlExpressionType.FunctionSelector:
                    var builder = new StringBuilder();
                    builder.Append(selector.Function.Name.ToLower());
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
            builder.Append(_translations[relation.Selector]);

            switch ((CqlExpressionType)relation.NodeType)
            {
                case CqlExpressionType.Equal:
                    builder.Append("=");
                    builder.Append(_translations[relation.Term]);
                    break;
                case CqlExpressionType.LargerEqualThan:
                    builder.Append(">=");
                    builder.Append(_translations[relation.Term]);
                    break;
                case CqlExpressionType.LargerThan:
                    builder.Append(">");
                    builder.Append(_translations[relation.Term]);
                    break;
                case CqlExpressionType.SmallerEqualThan:
                    builder.Append("<=");
                    builder.Append(_translations[relation.Term]);
                    break;
                case CqlExpressionType.SmallerThan:
                    builder.Append("<");
                    builder.Append(_translations[relation.Term]);
                    break;
                case CqlExpressionType.In:
                    builder.Append(" IN ");

                    if(((CqlExpressionType)relation.Term.NodeType == CqlExpressionType.Variable))
                    {
                        builder.Append(_translations[relation.Term]);
                    }
                    else
                    {
                        builder.Append("(");
                        var elements = relation.Term.Terms.Select(term => _translations[term]);
                        builder.Append(string.Join(",", elements));
                        builder.Append(")");
                    }

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
            base.VisitTerm(term);

            var builder = new StringBuilder();

            switch ((CqlExpressionType)term.NodeType)
            {
                case CqlExpressionType.Variable:
                    builder.Append("?");
                    break;

                case CqlExpressionType.Constant:
                    builder.Append(TypeSystem.ToStringValue(term.Value, CqlType.CreateType(term.Type)));
                    break;

                case CqlExpressionType.List:
                    {
                        builder.Append("[");
                        var elements = term.Terms.Select(value => _translations[value]).ToList();
                        builder.Append(string.Join(",", elements));
                        builder.Append("]");
                    }
                    break;

                case CqlExpressionType.Set:
                    {
                        builder.Append("{");
                        var elements = term.Terms.Select(value => _translations[value]).ToList();
                        builder.Append(string.Join(",", elements));
                        builder.Append("}");
                    }
                    break;

                case CqlExpressionType.Map:
                    {
                        builder.Append("{");
                        var elements =
                            term.DictionaryTerms.Select(
                                pair => string.Format("{0}:{1}", _translations[pair.Key], _translations[pair.Value])).
                                ToList();
                        builder.Append(string.Join(",", elements));
                        builder.Append("}");
                    }
                    break;

                case CqlExpressionType.Function:
                    builder.Append(term.Function.Name.ToLower());
                    builder.Append("(");
                    builder.Append(string.Join(",", term.Terms.Select(arg => _translations[arg])));
                    builder.Append(")");
                    break;

                default:
                    throw new CqlLinqException("Unexpected type of term encountered: " + term.NodeType.ToString());
            }

            _translations[term] = builder.ToString();
            return term;
        }


        public override Expression VisitOrdering(OrderingExpression ordering)
        {
            string value;

            switch ((CqlExpressionType)ordering.NodeType)
            {
                case CqlExpressionType.OrderDescending:
                    value = _translations[ordering.Selector] + " DESC";
                    break;
                case CqlExpressionType.OrderAscending:
                    value = _translations[ordering.Selector] + " ASC";
                    break;
                default:
                    throw new CqlLinqException("Unexpected ordering type encountered: " + ordering.NodeType.ToString());
            }

            _translations[ordering] = value;

            return ordering;
        }
    }
}