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

namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    ///   State of a tracked object
    /// </summary>
    public enum EntityState
    {
        /// <summary>
        ///   Indicates that an entity has not changed any values
        /// </summary>
        Unchanged,

        /// <summary>
        ///   Indicates that an entity has been added to a table
        /// </summary>
        Added,

        /// <summary>
        ///   Indicates that an entity has different values compared to the loaded database values
        /// </summary>
        Modified,

        /// <summary>
        ///   Indicates that an entity has been deleted from a table
        /// </summary>
        Deleted,

        /// <summary>
        ///   Indicates that an entity has no longer refers to a row in a database table
        /// </summary>
        Detached
    }
}