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

using System.Data.Common;

namespace CqlSharp
{
    /// <summary>
    ///   Provider factory than return the CqlSharp ADO object instances
    /// </summary>
    internal class CqlProviderFactory : DbProviderFactory
    {
        /// <summary>
        ///   Specifies whether the specific <see cref="T:System.Data.Common.DbProviderFactory" /> supports the <see
        ///    cref="T:System.Data.Common.DbDataSourceEnumerator" /> class.
        /// </summary>
        /// <returns> true if the instance of the <see cref="T:System.Data.Common.DbProviderFactory" /> supports the <see
        ///    cref="T:System.Data.Common.DbDataSourceEnumerator" /> class; otherwise false. </returns>
        public override bool CanCreateDataSourceEnumerator
        {
            get { return false; }
        }

        /// <summary>
        ///   Returns a new instance of the provider's class that implements the <see cref="T:System.Data.Common.DbCommand" /> class.
        /// </summary>
        /// <returns> A new instance of <see cref="T:System.Data.Common.DbCommand" /> . </returns>
        public override DbCommand CreateCommand()
        {
            return new CqlCommand();
        }

        /// <summary>
        ///   Returns a new instance of the provider's class that implements the <see cref="T:System.Data.Common.DbConnection" /> class.
        /// </summary>
        /// <returns> A new instance of <see cref="T:CqlSharp.CqlConnection" /> . </returns>
        public override DbConnection CreateConnection()
        {
            return new CqlConnection();
        }

        /// <summary>
        ///   Returns a new instance of the provider's class that implements the <see
        ///    cref="T:System.Data.Common.DbConnectionStringBuilder" /> class.
        /// </summary>
        /// <returns> A new instance of <see cref="T:CqlSharp.CqlConnectionStringBuilder" /> . </returns>
        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
            return new CqlConnectionStringBuilder();
        }

        /// <summary>
        ///   Returns a new instance of the provider's class that implements the <see cref="T:System.Data.Common.DbDataAdapter" /> class.
        /// </summary>
        /// <returns> A new instance of <see cref="T:CqlSharp.CqlDataAdapter" /> . </returns>
        public override DbDataAdapter CreateDataAdapter()
        {
            return new CqlDataAdapter();
        }

        /// <summary>
        ///   Returns a new instance of the provider's class that implements the <see cref="T:System.Data.Common.DbParameter" /> class.
        /// </summary>
        /// <returns> A new instance of <see cref="T:CqlSharp.CqlParameter" /> . </returns>
        public override DbParameter CreateParameter()
        {
            return new CqlParameter();
        }
    }
}