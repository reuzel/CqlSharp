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

namespace CqlSharp
{
    /// <summary>
    ///   A set of input paramaters used for the execution of prepared statements
    /// </summary>
    public class CqlParameterCollection
    {
        private readonly CqlSchema _schema;
        private byte[][] _values;

        internal CqlParameterCollection(CqlSchema schema)
        {
            _schema = schema;
            _values = new byte[schema.Count][];
        }

        public object this[int index]
        {
            get
            {
                if (_values[index] == null)
                    return null;

                return _schema[index].Deserialize(_values[index]);
            }
            set { _values[index] = _schema[index].Serialize(value); }
        }

        public object this[string name]
        {
            get
            {
                int index = _schema[name].Index;
                return this[index];
            }
            set
            {
                int index = _schema[name].Index;
                this[index] = value;
            }
        }

        internal byte[][] Values
        {
            get { return (byte[][]) _values.Clone(); }
        }

        /// <summary>
        ///   Sets the parameters to the values as defined by the properties of the provided object.
        /// </summary>
        /// <typeparam name="T"> Type of the object holding the parameter values. The names of the properties must match the names of the columns of the schema of the prepared query for them to be usable. </typeparam>
        /// <param name="source"> The object holding the parameter values. </param>
        public void Set<T>(T source)
        {
            ObjectAccessor accessor = ObjectAccessor.GetAccessor<T>();
            foreach (CqlColumn column in _schema)
            {
                object value;
                if (accessor.TryGetValue(column, source, out value))
                    this[column.Index] = value;
            }
        }

        /// <summary>
        ///   Clears the parameter values
        /// </summary>
        public void Clear()
        {
            _values = new byte[_schema.Count][];
        }
    }
}