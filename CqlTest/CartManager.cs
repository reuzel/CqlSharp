// CqlSharp - CqlTest
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
using System.Threading.Tasks;
using CqlSharp;
using CqlSharp.Protocol;

namespace CqlTest
{
    internal class CartManager
    {
        /// <summary>
        ///   Prepares the db async.
        /// </summary>
        /// <returns> </returns>
        public async Task PrepareDbAsync()
        {
            using (var connection = new CqlConnection("cartDB"))
            {
                await connection.OpenAsync();

                try
                {
                    var shopExistCommand = new CqlCommand(connection,
                                                          "select * from system.schema_keyspaces where keyspace_name='shop';");
                    using (var reader = shopExistCommand.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            var dropKs = new CqlCommand(connection, @"DROP KEYSPACE Shop;");
                            await dropKs.ExecuteNonQueryAsync();
                        }
                    }
                }
                catch (AlreadyExistsException)
                {
                    //ignore
                }

                try
                {
                    //create the keyspace if it does not exist
                    var createKs = new CqlCommand(connection,
                                                  @"CREATE KEYSPACE Shop WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = 'false';");
                    await createKs.ExecuteNonQueryAsync();

                    //create the table
                    var createTable = new CqlCommand(connection,
                                                     @"create table Shop.Carts (id uuid primary key, groupId text, items map<text, int>);");
                    await createTable.ExecuteNonQueryAsync();

                    //create corresponding index
                    var createIndex = new CqlCommand(connection, @"create index on Shop.Carts(GroupId)");
                    await createIndex.ExecuteNonQueryAsync();
                }
                catch (AlreadyExistsException)
                {
                }
            }
        }

        /// <summary>
        ///   Creates the cart async.
        /// </summary>
        /// <returns> </returns>
        public async Task<Cart> AddCartAsync(string groupId)
        {
            var c = new Cart {Id = Guid.NewGuid(), GroupId = groupId};

            using (var connection = new CqlConnection("cartDB"))
            {
                await connection.OpenAsync();

                var command = new CqlCommand(connection, "insert into Shop.Carts (id, groupId) values (?,?);");
                await command.PrepareAsync();
                command.Parameters.Set(c);
                await command.ExecuteNonQueryAsync();
                return c;
            }
        }

        /// <summary>
        ///   Updates the cart async.
        /// </summary>
        /// <param name="c"> The c. </param>
        /// <returns> </returns>
        public async Task UpdateCartAsync(Cart c)
        {
            using (var connection = new CqlConnection("cartDB"))
            {
                await connection.OpenAsync();

                var command = new CqlCommand(connection, "update Shop.Carts set groupid=?, items=? where id=?;");
                await command.PrepareAsync();
                command.Parameters.Set(c);
                await command.ExecuteNonQueryAsync();
            }
        }

        /// <summary>
        ///   Queries the cart async.
        /// </summary>
        /// <param name="guid"> The GUID. </param>
        /// <returns> </returns>
        public async Task<Dictionary<string, int>> GetItemsAsync(Guid guid)
        {
            using (var connection = new CqlConnection("cartDB"))
            {
                await connection.OpenAsync();

                var command = new CqlCommand(connection, "select Items from Shop.Carts where id=?;");
                await command.PrepareAsync();
                command.UseBuffering = true;
                command.Parameters["id"] = guid;
                using (var reader = await command.ExecuteReaderAsync())
                {
                    return await reader.ReadAsync() ? (Dictionary<string, int>) reader["items"] : null;
                }
            }
        }


        /// <summary>
        ///   Adds the items async.
        /// </summary>
        /// <param name="guid"> The GUID. </param>
        /// <param name="items"> The items. </param>
        /// <returns> </returns>
        public async Task AddItemsAsync(Guid guid, Dictionary<string, int> items)
        {
            using (var connection = new CqlConnection("cartDB"))
            {
                await connection.OpenAsync();

                var command = new CqlCommand(connection, "update Shop.Carts set items = items + ? where id = ?;");
                await command.PrepareAsync();
                command.Parameters["id"] = guid;
                command.Parameters["items"] = items;
                await command.ExecuteNonQueryAsync();
            }
        }


        /// <summary>
        ///   Finds the carts by group id async.
        /// </summary>
        /// <param name="groupId"> The group id. </param>
        /// <returns> </returns>
        public async Task<List<Cart>> FindCartsByGroupIdAsync(string groupId)
        {
            using (var connection = new CqlConnection("cartDB"))
            {
                await connection.OpenAsync();

                var command = new CqlCommand(connection, "select * from Shop.Carts where GroupId=?;");
                await command.PrepareAsync();
                command.UseBuffering = true;
                command.Parameters["groupid"] = groupId;
                using (var reader = await command.ExecuteReaderAsync<Cart>())
                {
                    var carts = new List<Cart>();
                    while (await reader.ReadAsync())
                    {
                        carts.Add(reader.Current);
                    }
                    return carts;
                }
            }
        }
    }
}