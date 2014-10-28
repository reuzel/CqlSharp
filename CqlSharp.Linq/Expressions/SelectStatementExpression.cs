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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace CqlSharp.Linq.Expressions
{
    /// <summary>
    ///   Represents a select query
    /// </summary>
    internal class SelectStatementExpression : Expression
    {
        private readonly bool _allowFiltering;
        private readonly int? _limit;
        private readonly ReadOnlyCollection<OrderingExpression> _orderBy;
        private readonly SelectClauseExpression _selectClause;
        private readonly string _tableName;
        private readonly Type _type;
        private readonly ReadOnlyCollection<RelationExpression> _whereClause;

        public SelectStatementExpression(Type type, SelectClauseExpression selectClause, string tableName,
                                         IList<RelationExpression> whereClause, IList<OrderingExpression> orderBy,
                                         int? limit, bool allowFiltering)
        {
            if (type == null)
                throw new ArgumentNullException("type");

            if (selectClause == null)
                throw new ArgumentNullException("selectClause");

            if (tableName == null)
                throw new ArgumentNullException("tableName");

            _type = type;
            _selectClause = selectClause;
            _tableName = tableName;
            _whereClause = whereClause.AsReadOnly();
            _orderBy = orderBy.AsReadOnly();
            _limit = limit;
            _allowFiltering = allowFiltering;
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType) CqlExpressionType.SelectStatement; }
        }

        public override Type Type
        {
            get { return _type; }
        }

        public SelectClauseExpression SelectClause
        {
            get { return _selectClause; }
        }

        public string TableName
        {
            get { return _tableName; }
        }

        public ReadOnlyCollection<RelationExpression> WhereClause
        {
            get { return _whereClause; }
        }

        public ReadOnlyCollection<OrderingExpression> OrderBy
        {
            get { return _orderBy; }
        }

        public int? Limit
        {
            get { return _limit; }
        }

        public bool AllowFiltering
        {
            get { return _allowFiltering; }
        }

        protected override Expression Accept(ExpressionVisitor visitor)
        {
            var type = visitor as CqlExpressionVisitor;

            if (type != null)
            {
                return type.VisitSelectStatement(this);
            }

            return base.Accept(visitor);
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            bool changed = false;

            var selectClause = (SelectClauseExpression) visitor.Visit(_selectClause);
            changed |= selectClause != _selectClause;

            int count = _whereClause.Count;
            var wheres = new RelationExpression[count];
            for (int i = 0; i < count; i++)
            {
                wheres[i] = (RelationExpression) visitor.Visit(_whereClause[i]);
                changed |= wheres[i] != _whereClause[i];
            }

            count = _orderBy.Count;
            var order = new OrderingExpression[count];
            for (int i = 0; i < count; i++)
            {
                order[i] = (OrderingExpression) visitor.Visit(_orderBy[i]);
                changed |= order[i] != _orderBy[i];
            }

            return changed
                       ? new SelectStatementExpression(_type, selectClause, _tableName, wheres, order, _limit,
                                                       _allowFiltering)
                       : this;
        }
    }
}