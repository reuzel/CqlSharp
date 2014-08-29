// CqlSharp - CqlSharp.Performance.Data
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
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp.Performance.Data
{
    /// <summary>
    /// Provides access to the Measurements of different customers
    /// </summary>
    public static class MeasurementManager
    {
        private const string ConnectionString = "Measurements";
        private static readonly Random Random = new Random();

        public static readonly List<string> Customers = new List<string>
        {
            "Joost",
            "Sander",
            "Erik",
            "Klaas",
            "Willem",
            "Kees",
            "Jan",
            "Piet"
        };

        /// <summary>
        /// Creates the database.
        /// </summary>
        public static void CreateDatabase()
        {
            using(var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                var existCommand = new CqlCommand(connection,
                                                  "select * from system.schema_keyspaces where keyspace_name='cqlsharptest';");
                using(var reader = existCommand.ExecuteReader())
                {
                    //check if database exists, if so, use it, otherwise create it
                    if(!reader.Read())
                    {
                        //create the keyspace 
                        var createKs = new CqlCommand(connection,
                                                      @"CREATE KEYSPACE cqlsharptest WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};");
                        createKs.ExecuteNonQuery();

                        //create the table
                        var createTable = new CqlCommand(connection,
                                                         @"CREATE TABLE cqlsharptest.measurements (id int PRIMARY KEY, customer text, values map<text, int>);");
                        createTable.ExecuteNonQuery();

                        //fill table with dummy data
                        var inserts = new List<Task>(25000);
                        for(int i = 0; i < 25000; i++)
                        {
                            //create and prepare the insert command
                            var insertCommand = new CqlCommand(connection,
                                                               "insert into cqlsharptest.measurements (id, customer, values) values (?,?,?)");
                            insertCommand.Prepare();

                            //create new measurement
                            var measurement = new Measurement
                            {
                                Id = i,
                                Customer = Customers[Random.Next(0, Customers.Count)],
                                Values = new Dictionary<string, int>
                                {
                                    {"Temp", Random.Next(0, 101)},
                                    {"Humidity", Random.Next(0, 101)},
                                    {"Clouds", Random.Next(0, 101)},
                                    {"Pressure", Random.Next(0, 101)},
                                    {"Rain", Random.Next(0, 101)},
                                    {"Sunshine", Random.Next(0, 101)},
                                    {"Overall", Random.Next(0, 101)}
                                }
                            };

                            //set insert parameters
                            insertCommand.Parameters.Set(measurement);

                            //add the insert as task to list
                            inserts.Add(insertCommand.ExecuteNonQueryAsync());
                        }

                        //wait for all inserts to complete
                        Task.WaitAll(inserts.ToArray());
                    }
                }
            }
        }

        /// <summary>
        /// Gets the measurement.
        /// </summary>
        /// <param name="id">The identifier.</param>
        /// <returns></returns>
        public static Measurement GetMeasurement(int id)
        {
            //return GetMeasurementAsync(id).Result;
            
            using(var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                var selectCommand = new CqlCommand(connection, "select * from cqlsharptest.measurements where id=?");
                selectCommand.CommandTimeout = Timeout.Infinite;
                selectCommand.Prepare();

                selectCommand.Parameters[0].Value = id;

                using(var reader = selectCommand.ExecuteReader<Measurement>())
                {
                    return reader.Read() ? reader.Current : null;
                }
            }
        }

        /// <summary>
        /// Gets the measurement asynchronous.
        /// </summary>
        /// <param name="id">The identifier.</param>
        /// <returns></returns>
        public static async Task<Measurement> GetMeasurementAsync(int id)
        {
            using(var connection = new CqlConnection(ConnectionString))
            {
                await connection.OpenAsync();

                var selectCommand = new CqlCommand(connection, "select * from cqlsharptest.measurements where id=?");
                await selectCommand.PrepareAsync();

                selectCommand.Parameters[0].Value = id;

                using(var reader = await selectCommand.ExecuteReaderAsync<Measurement>())
                {
                    return (await reader.ReadAsync()) ? reader.Current : null;
                }
            }
        }

        public static void Disconnect()
        {
            CqlConnection.Shutdown(ConnectionString);
        }
    }
}