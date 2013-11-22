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
        private readonly int? _limit;
        private readonly ReadOnlyCollection<Expression> _orderBy;
        private readonly Expression _selectClause;
        private readonly string _tableName;
        private readonly Type _type;
        private readonly ReadOnlyCollection<Expression> _whereClause;

        public SelectStatementExpression(Type type, Expression selectClause, string tableName,
                                IList<Expression> whereClause, IList<Expression> orderBy,
                                int? limit)
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

            if (_whereClause != null)
                _whereClause = new ReadOnlyCollection<Expression>(whereClause);

            if (_orderBy != null)
                _orderBy = new ReadOnlyCollection<Expression>(orderBy);

            _limit = limit;
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)CqlExpressionType.SelectStatement; }
        }

        public override Type Type
        {
            get { return _type; }
        }

        public Expression SelectClause
        {
            get { return _selectClause; }
        }

        public string TableName
        {
            get { return _tableName; }
        }

        public ReadOnlyCollection<Expression> WhereClause
        {
            get { return _whereClause; }
        }

        public ReadOnlyCollection<Expression> OrderBy
        {
            get { return _orderBy; }
        }

        public int? Limit
        {
            get { return _limit; }
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

            Expression selectClause = visitor.Visit(_selectClause);
            changed |= selectClause != _selectClause;

            Expression[] wheres = null;
            if (_whereClause != null)
            {
                int count = _whereClause.Count;
                wheres = new Expression[count];
                for (int i = 0; i < count; i++)
                {
                    wheres[i] = visitor.Visit(_whereClause[i]);
                    changed |= wheres[i] != _whereClause[i];
                }
            }

            Expression[] order = null;
            if (_orderBy != null)
            {
                int count = _orderBy.Count;
                order = new Expression[count];
                for (int i = 0; i < count; i++)
                {
                    order[i] = visitor.Visit(_orderBy[i]);
                    changed |= order[i] != _orderBy[i];
                }
            }

            return changed ? new SelectStatementExpression(_type, selectClause, _tableName, wheres, order, _limit) : this;
        }
    }
}