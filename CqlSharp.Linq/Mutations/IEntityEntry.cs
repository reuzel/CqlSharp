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

namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    ///   Provides non-generic access to a tracked entity
    /// </summary>
    public interface IEntityEntry
    {
        /// <summary>
        ///   Gets the table this entity is part of.
        /// </summary>
        /// <value> The table. </value>
        ICqlTable Table { get; }

        /// <summary>
        ///   Gets the key that uniquely identifies this entity within the table.
        /// </summary>
        /// <value> The key. </value>
        IEntityKey Key { get; }

        /// <summary>
        ///   Gets the current entity values
        /// </summary>
        /// <value> The entity. </value>
        Object Entity { get; }

        /// <summary>
        ///   Gets the original values as last read from the database.
        /// </summary>
        /// <value> The original. </value>
        Object Original { get; }

        /// <summary>
        ///   Gets the modification state of this entity .
        /// </summary>
        /// <value> The state. </value>
        EntityState State { get; }

        /// <summary>
        ///   Sets the original values of this entity.
        /// </summary>
        /// <param name="newOriginal"> The new original. </param>
        void SetOriginalValues(object newOriginal);

        /// <summary>
        ///   Sets the object values.
        /// </summary>
        /// <param name="newValues"> The new values. </param>
        void SetEntityValues(object newValues);

        /// <summary>
        ///   Reloads this instance from the database, effectively making it unchanged.
        /// </summary>
        void Reload();
    }
}