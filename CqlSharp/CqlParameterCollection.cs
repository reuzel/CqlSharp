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

using CqlSharp.Protocol;
using CqlSharp.Serialization;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;

namespace CqlSharp
{
    /// <summary>
    /// A collection of CqlParameters to be used with CqlCommands
    /// </summary>
    public class CqlParameterCollection : KeyedCollection<string, CqlParameter>, IDataParameterCollection
    {
        public CqlParameterCollection()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlParameterCollection" /> class.
        /// </summary>
        /// <param name="columns">The columns.</param>
        /// <param name="option">The parameter creation option.</param>
        internal CqlParameterCollection(IEnumerable<Column> columns, CqlParameterCreationOption option)
        {
            if (option == CqlParameterCreationOption.None)
                return;

            foreach (var column in columns)
            {
                var parameter = new CqlParameter();
                switch (option)
                {
                    case CqlParameterCreationOption.Column:
                        parameter.ParameterName = column.Name;
                        break;
                    case CqlParameterCreationOption.Table:
                        parameter.ParameterName = column.TableAndName;
                        break;
                    case CqlParameterCreationOption.KeySpace:
                        parameter.ParameterName = column.KeySpaceTableAndName;
                        break;
                }

                parameter.CqlType = column.CqlType;
                parameter.CollectionKeyType = column.CollectionKeyType;
                parameter.CollectionValueType = column.CollectionValueType;

                Add(parameter);
            }
        }

        protected override string GetKeyForItem(CqlParameter item)
        {
            return item.ParameterName;
        }

        public int IndexOf(string parameterName)
        {
            return IndexOf(base[parameterName]);
        }

        public void RemoveAt(string parameterName)
        {
            Remove(parameterName);
        }

        object IDataParameterCollection.this[string parameterName]
        {
            get { return this[parameterName]; }
            set { SetItem(IndexOf(parameterName), (CqlParameter)value); }
        }

        internal byte[][] Values
        {
            get
            {
                var values = new byte[Count][];
                for (int i = 0; i < Count; i++)
                {
                    CqlParameter param = this[i];

                    if (param.Value == DBNull.Value) continue;

                    values[i] = ValueSerialization.Serialize(param.CqlType, param.CollectionKeyType,
                                                             param.CollectionValueType, param.Value);
                }

                return values;
            }
        }

        /// <summary>
        ///   Sets the parameters to the values as defined by the properties of the provided object.
        /// </summary>
        /// <typeparam name="T"> Type of the object holding the parameter values. The names of the properties must match the names of the columns of the schema of the prepared query for them to be usable. </typeparam>
        /// <param name="source"> The object holding the parameter values. </param>
        public void Set<T>(T source)
        {
            ObjectAccessor<T> accessor = ObjectAccessor<T>.Instance;
            foreach (var parameter in Items)
            {
                object value;
                if (accessor.TryGetValue(parameter.ParameterName, source, out value))
                {
                    parameter.Value = value;
                }
            }
        }
    }
}