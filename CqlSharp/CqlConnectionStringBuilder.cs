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

using CqlSharp.Logging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Net.Sockets;

namespace CqlSharp
{
    /// <summary>
    /// Enables parsing of the Connection String
    /// </summary>
    public class CqlConnectionStringBuilder : DbConnectionStringBuilder
    {
        /// <summary>
        ///   The default values for a CqlConnection string
        /// </summary>
        private static readonly Dictionary<Keyword, object> DefaultValues
            = new Dictionary<Keyword, object>
                  {
                      {Keyword.Servers, ""},
                      {Keyword.Port, 9042},
                      {Keyword.Username, null},
                      {Keyword.Password, null},
                      {Keyword.DiscoveryScope, DiscoveryScope.None},
                      {Keyword.ConnectionStrategy, ConnectionStrategy.Balanced },
                      {Keyword.CqlVersion, "3.0.2"},
                      {Keyword.MaxDownTime, 60*60*1000}, //node max down for 1 hour
                      {Keyword.MinDownTime, 500}, //Node min down for 500 milli sec
                      {Keyword.MaxConnectionsPerNode, 2},
                      {Keyword.MaxConnections, -1},
                      {Keyword.NewConnectionTreshold, 10},
                      {Keyword.MaxConcurrentQueries, -1},
                      {Keyword.MaxConnectionIdleTime, 10},
                      {Keyword.MaxQueryRetries, 3},
                      {Keyword.LoggerFactory, "Null"},
                      {Keyword.LogLevel, LogLevel.Info},
                      {Keyword.UseBuffering, true},
                      {Keyword.AllowCompression, true},
                      {Keyword.CompressionTreshold, 2048}
                  };

        /// <summary>
        ///   The mapping of connection string strings to allowed keywords
        /// </summary>
        private static readonly Dictionary<string, Keyword> Keywords =
            new Dictionary<string, Keyword>(StringComparer.OrdinalIgnoreCase)
                {
                    {"servers", Keyword.Servers},
                    {"server", Keyword.Servers},
                    {"nodes", Keyword.Servers},
                    {"node", Keyword.Servers},
                    {"port", Keyword.Port},
                    {"discovery scope", Keyword.DiscoveryScope},
                    {"discoveryscope", Keyword.DiscoveryScope},
                    {"connection strategy", Keyword.ConnectionStrategy},
                    {"connectionstrategy", Keyword.ConnectionStrategy},
                    {"cql", Keyword.CqlVersion},
                    {"version", Keyword.CqlVersion},
                    {"cql version", Keyword.CqlVersion},
                    {"cqlversion", Keyword.CqlVersion},
                    {"user name", Keyword.Username},
                    {"user id", Keyword.Username},
                    {"username", Keyword.Username},
                    {"user", Keyword.Username},
                    {"password", Keyword.Password},
                    {"max downtime", Keyword.MaxDownTime},
                    {"maxdowntime", Keyword.MaxDownTime},
                    {"min downtime", Keyword.MinDownTime},
                    {"mindowntime", Keyword.MinDownTime},
                    {"max connections", Keyword.MaxConnections},
                    {"maxconnections", Keyword.MaxConnections},
                    {"connections per node", Keyword.MaxConnectionsPerNode},
                    {"max connections per node", Keyword.MaxConnectionsPerNode},
                    {"maxconnectionspernode", Keyword.MaxConnectionsPerNode},
                    {"connectionspernode", Keyword.MaxConnectionsPerNode},
                    {"maxpernode", Keyword.MaxConnectionsPerNode},
                    {"max per node", Keyword.MaxConnectionsPerNode},
                    {"newconnectiontreshold", Keyword.NewConnectionTreshold},
                    {"new connection treshold", Keyword.NewConnectionTreshold},
                    {"treshold", Keyword.NewConnectionTreshold},
                    {"connection treshold", Keyword.NewConnectionTreshold},
                    {"maxconcurrentqueries", Keyword.MaxConcurrentQueries},
                    {"max concurrent queries", Keyword.MaxConcurrentQueries},
                    {"max concurrent", Keyword.MaxConcurrentQueries},
                    {"concurrent queries", Keyword.MaxConcurrentQueries},
                    {"concurrentqueries", Keyword.MaxConcurrentQueries},
                    {"max queries", Keyword.MaxConcurrentQueries},
                    {"maxqueries", Keyword.MaxConcurrentQueries},
                    {"throttle", Keyword.MaxConcurrentQueries},
                    {"maxconnectionidletime", Keyword.MaxConnectionIdleTime},
                    {"max connection idle time", Keyword.MaxConnectionIdleTime},
                    {"connectionidletime", Keyword.MaxConnectionIdleTime},
                    {"connection idle time", Keyword.MaxConnectionIdleTime},
                    {"maxidletime", Keyword.MaxConnectionIdleTime},
                    {"max idle time", Keyword.MaxConnectionIdleTime},
                    {"retries", Keyword.MaxQueryRetries},
                    {"queryretries", Keyword.MaxQueryRetries},
                    {"query retries", Keyword.MaxQueryRetries},
                    {"maxretries", Keyword.MaxQueryRetries},
                    {"max retries", Keyword.MaxQueryRetries},
                    {"maxqueryretries", Keyword.MaxQueryRetries},
                    {"max query retries", Keyword.MaxQueryRetries},
                    {"logger", Keyword.LoggerFactory},
                    {"loggerfactory", Keyword.LoggerFactory},
                    {"logger factory", Keyword.LoggerFactory},
                    {"loglevel", Keyword.LogLevel},
                    {"level", Keyword.LogLevel},
                    {"log level", Keyword.LogLevel},
                    {"log", Keyword.LogLevel},
                    {"buffering", Keyword.UseBuffering},
                    {"use buffering", Keyword.UseBuffering},
                    {"usebuffering", Keyword.UseBuffering},
                    {"compression", Keyword.AllowCompression},
                    {"allow compression", Keyword.AllowCompression},
                    {"enable compression", Keyword.AllowCompression},
                    {"support compression", Keyword.AllowCompression},
                    {"allowcompression", Keyword.AllowCompression},
                    {"enablecompression", Keyword.AllowCompression},
                    {"supportcompression", Keyword.AllowCompression},
                    {"compressiontreshold", Keyword.CompressionTreshold},
                    {"compression treshold", Keyword.CompressionTreshold},
                    {"compressionsize", Keyword.CompressionTreshold},
                    {"compression size", Keyword.CompressionTreshold},
                    {"min compression size", Keyword.CompressionTreshold},
                    {"mincompressionsize", Keyword.CompressionTreshold}
                };

        /// <summary>
        ///   The current set of values
        /// </summary>
        private Dictionary<Keyword, object> _values = new Dictionary<Keyword, object>(DefaultValues);

        private Dictionary<string, IPAddress> _nodeAddresses;

        public CqlConnectionStringBuilder()
        {
        }

        public CqlConnectionStringBuilder(string connectionString)
        {
            if (connectionString == null)
                throw new ArgumentNullException("connectionString");

            var fromConfig = ConfigurationManager.ConnectionStrings[connectionString];
            ConnectionString = fromConfig != null ? fromConfig.ConnectionString : connectionString;
        }

        /// <summary>
        ///   Gets or sets the value associated with the specified key.
        /// </summary>
        /// <param name="keyword"> The keyword. </param>
        /// <returns> </returns>

        public override object this[string keyword]
        {
            get
            {
                Keyword k;
                if (Keywords.TryGetValue(keyword, out k))
                {
                    return _values[k];
                }
                return null;
            }
            set
            {
                Keyword k;
                if (Keywords.TryGetValue(keyword, out k))
                {
                    SetValue(k, value);
                }
            }
        }

        /// <summary>
        ///   Gets or sets a list of addresses or ip's pointing to nodes in Cassandra cluster.
        /// </summary>
        /// <value> The IP-addresses or DNS names of the nodes. </value>
        public IEnumerable<string> Servers
        {
            get
            {
                var servers = (string)_values[Keyword.Servers];
                return servers.Split(',');
            }
            set
            {
                SetValue(Keyword.Servers, value);
            }
        }

        /// <summary>
        ///   Gets the node addresses.
        /// </summary>
        /// <value> The node addresses. </value>
        /// <exception cref="CqlException">Can not obtain a valid IP-Address from a node specified in the configuration</exception>
        public IEnumerable<IPAddress> ServerAddresses
        {
            get
            {
                if (_nodeAddresses == null)
                    _nodeAddresses = new Dictionary<string, IPAddress>();

                foreach (string nameOrAddress in Servers)
                {
                    IPAddress address;

                    if (!_nodeAddresses.TryGetValue(nameOrAddress, out address))
                    {
                        try
                        {
                            if (!IPAddress.TryParse(nameOrAddress, out address))
                            {
                                address =
                                    Dns.GetHostAddresses(
                                        nameOrAddress).
                                        FirstOrDefault(
                                            addr =>
                                            addr.AddressFamily ==
                                            AddressFamily.
                                                InterNetwork);
                            }

                            if (address != null)
                                _nodeAddresses.Add(nameOrAddress, address);
                        }
                        catch (Exception ex)
                        {
                            throw new CqlException(
                                "Can not obtain a valid IP-Address from a node specified in the configuration", ex);
                        }
                    }

                    if (address != null)
                        yield return address;
                }
            }
        }

        /// <summary>
        /// Gets or sets the port to use to connect to the Cassandra cluster.
        /// </summary>
        /// <value>
        /// The port. Default: 9042
        /// </value>
        public int Port
        {
            get { return (int)_values[Keyword.Port]; }
            set { SetValue(Keyword.Port, value); }
        }

        /// <summary>
        ///   Gets or sets the discovery scope. Used to enlarge the list of nodes to which connections can be made
        /// </summary>
        /// <value> The discovery scope. Default: None </value>
        public DiscoveryScope DiscoveryScope
        {
            get { return (DiscoveryScope)_values[Keyword.DiscoveryScope]; }
            set { SetValue(Keyword.DiscoveryScope, value); }
        }


        /// <summary>
        ///   Gets or sets the connection strategy used to connect to nodes.
        /// </summary>
        /// <value> The connection strategy. Default: Balanced </value>
        public ConnectionStrategy ConnectionStrategy
        {
            get { return (ConnectionStrategy)_values[Keyword.ConnectionStrategy]; }
            set { SetValue(Keyword.ConnectionStrategy, value); }
        }

        /// <summary>
        ///   Gets or sets the username used to connect to the Cassandra cluster
        /// </summary>
        /// <value> The username. Default: null </value>
        public string Username
        {
            get { return (string)_values[Keyword.Username]; }
            set { SetValue(Keyword.Username, value); }
        }

        /// <summary>
        ///   Gets or sets the password.
        /// </summary>
        /// <value> The password. Default: null </value>
        public string Password
        {
            get { return (string)_values[Keyword.Password]; }
            set { SetValue(Keyword.Password, value); }
        }

        /// <summary>
        ///   Gets or sets the CQL version.
        /// </summary>
        /// <value> The CQL version. Default: 3.0.0 </value>
        public string CqlVersion
        {
            get { return (string)_values[Keyword.CqlVersion]; }
            set { SetValue(Keyword.CqlVersion, value); }
        }

        /// <summary>
        ///   Gets or sets the max down time of a cassandra node, when connections failed. When a node is marked down,
        ///   no new connection attempts are made.
        /// </summary>
        /// <value> The max down time in ms. Default: 1 hour </value>
        public int MaxDownTime
        {
            get { return (int)_values[Keyword.MaxDownTime]; }
            set { SetValue(Keyword.MaxDownTime, value); }
        }

        /// <summary>
        ///   Gets or sets the min down time of a node when a connection failed.
        /// </summary>
        /// <value> The min down time in ms. Default 500ms </value>
        public int MinDownTime
        {
            get { return (int)_values[Keyword.MinDownTime]; }
            set { SetValue(Keyword.MinDownTime, value); }
        }

        /// <summary>
        ///   Gets or sets the max connections per node.
        /// </summary>
        /// <value> The max connections per node. Default 2 </value>
        public int MaxConnectionsPerNode
        {
            get { return (int)_values[Keyword.MaxConnectionsPerNode]; }
            set { SetValue(Keyword.MaxConnectionsPerNode, value); }
        }

        /// <summary>
        ///   Gets or sets the max connections to the cluster.
        /// </summary>
        /// <value> The max connections. When 0 or negative, no maximum. Default: -1 </value>
        public int MaxConnections
        {
            get { return (int)_values[Keyword.MaxConnections]; }
            set { SetValue(Keyword.MaxConnections, value); }
        }

        /// <summary>
        ///   Gets or sets the new connection treshold. This threshold defines when a new connection is created
        ///   when other connections are "full"
        /// </summary>
        /// <value> The new connection treshold. Default: 10 </value>
        public int NewConnectionTreshold
        {
            get { return (int)_values[Keyword.NewConnectionTreshold]; }
            set { SetValue(Keyword.NewConnectionTreshold, value); }
        }

        /// <summary>
        ///   Gets or sets the max concurrent queries. Threads attempting to execute a query (whether async or not) will be blocked until
        ///   the number of active queries drops below this number.
        /// </summary>
        /// <value> The max concurrent queries. if 0 or negative, the max will be calculated by the number of found nodes in the cluster * MaxConnectionsPerNode * 2. </value>
        public int MaxConcurrentQueries
        {
            get { return (int)_values[Keyword.MaxConcurrentQueries]; }
            set { SetValue(Keyword.MaxConcurrentQueries, value); }
        }

        /// <summary>
        ///   Gets or sets the max connection idle time. Any connection that did not perform a query within this timespan is elligable to be closed.
        /// </summary>
        /// <value> The max connection idle time in seconds. Default 10 seconds. </value>
        public int MaxConnectionIdleTime
        {
            get { return (int)_values[Keyword.MaxConnectionIdleTime]; }
            set { SetValue(Keyword.MaxConnectionIdleTime, value); }
        }

        /// <summary>
        ///   Gets or sets the maximum amount of query retries.
        /// </summary>
        /// <value> The max query retries. Default 3. </value>
        public int MaxQueryRetries
        {
            get { return (int)_values[Keyword.MaxQueryRetries]; }
            set { SetValue(Keyword.MaxQueryRetries, value); }
        }

        /// <summary>
        ///   Gets or sets the name of the to be used logger factory.
        /// </summary>
        /// <value> The logger factory. </value>
        public string LoggerFactory
        {
            get { return (string)_values[Keyword.LoggerFactory]; }
            set { SetValue(Keyword.LoggerFactory, value); }
        }

        /// <summary>
        ///   Gets or sets the log level.
        /// </summary>
        /// <value> The log level. </value>
        public LogLevel LogLevel
        {
            get { return (LogLevel)_values[Keyword.LogLevel]; }
            set { SetValue(Keyword.LogLevel, value); }
        }

        /// <summary>
        ///   Gets or sets a value indicating whether to use buffering to load query responses.
        /// </summary>
        /// <value> <c>true</c> if [use buffering]; otherwise, <c>false</c> . </value>
        public bool UseBuffering
        {
            get { return (bool)_values[Keyword.UseBuffering]; }
            set { SetValue(Keyword.UseBuffering, value); }
        }

        /// <summary>
        ///   Gets or sets a value indicating whether to [allow compression].
        /// </summary>
        /// <value> <c>true</c> if [allow compression]; otherwise, <c>false</c> . </value>
        public bool AllowCompression
        {
            get { return (bool)_values[Keyword.AllowCompression]; }
            set { SetValue(Keyword.AllowCompression, value); }
        }

        /// <summary>
        ///   Gets or sets the compression treshold. Frames below this size will not be compressed.
        /// </summary>
        /// <value> The compression treshold. </value>
        public int CompressionTreshold
        {
            get { return (int)_values[Keyword.CompressionTreshold]; }
            set { SetValue(Keyword.CompressionTreshold, value); }
        }

        /// <summary>
        ///   Sets the value to the correct value, tries to parse the given value
        /// </summary>
        /// <param name="keyword"> The keyword. </param>
        /// <param name="value"> The value. </param>
        private void SetValue(Keyword keyword, object value)
        {
            Object sanatizedValue = null;
            if (value == null)
                sanatizedValue = DefaultValues[keyword];
            else
            {
                switch (keyword)
                {
                    case Keyword.Servers:
                        sanatizedValue = ConvertToEnumerationString(value);
                        _nodeAddresses = null;
                        break;
                    case Keyword.Port:
                        sanatizedValue = Convert.ToInt32(value);
                        break;
                    case Keyword.Username:
                        sanatizedValue = value.ToString();
                        break;
                    case Keyword.Password:
                        sanatizedValue = value.ToString();
                        break;
                    case Keyword.DiscoveryScope:
                        sanatizedValue = ConvertToEnum<DiscoveryScope>(value);
                        break;
                    case Keyword.ConnectionStrategy:
                        sanatizedValue = ConvertToEnum<ConnectionStrategy>(value);
                        break;
                    case Keyword.CqlVersion:
                        sanatizedValue = value.ToString();
                        break;
                    case Keyword.MaxDownTime:
                        sanatizedValue = Convert.ToInt32(value);
                        break;
                    case Keyword.MinDownTime:
                        sanatizedValue = Convert.ToInt32(value);
                        break;
                    case Keyword.MaxConnectionsPerNode:
                        sanatizedValue = Convert.ToInt32(value);
                        break;
                    case Keyword.MaxConnections:
                        sanatizedValue = Convert.ToInt32(value);
                        break;
                    case Keyword.NewConnectionTreshold:
                        sanatizedValue = Convert.ToInt32(value);
                        break;
                    case Keyword.MaxConcurrentQueries:
                        sanatizedValue = Convert.ToInt32(value);
                        break;
                    case Keyword.MaxConnectionIdleTime:
                        sanatizedValue = Convert.ToInt32(value);
                        break;
                    case Keyword.MaxQueryRetries:
                        sanatizedValue = Convert.ToInt32(value);
                        break;
                    case Keyword.LoggerFactory:
                        sanatizedValue = value.ToString();
                        break;
                    case Keyword.LogLevel:
                        sanatizedValue = ConvertToEnum<LogLevel>(value);
                        break;
                    case Keyword.UseBuffering:
                        sanatizedValue = ConvertToBoolean(value);
                        break;
                    case Keyword.AllowCompression:
                        sanatizedValue = ConvertToBoolean(value);
                        break;
                    case Keyword.CompressionTreshold:
                        sanatizedValue = Convert.ToInt32(value);
                        break;
                }
            }

            _values[keyword] = sanatizedValue;
            base[keyword.ToString()] = sanatizedValue == DefaultValues[keyword] ? null : sanatizedValue;
        }


        private static string ConvertToEnumerationString(object value)
        {
            var str = value as string;
            if (str != null) return str;

            var enumerable = value as IEnumerable;
            if (enumerable != null)
            {
                var cleanedElems = (from object elem in enumerable select elem.ToString().ToLower().Trim());
                return string.Join(",", cleanedElems);
            }

            return "";
        }

        private static T ConvertToEnum<T>(object value)
        {
            if (value == null)
                throw new ArgumentNullException("value");

            if (value is T)
                return (T)value;

            return (T)Enum.Parse(typeof(T), value.ToString(), true);
        }

        private static bool ConvertToBoolean(object value)
        {
            var str = value as string;
            if (str != null)
            {
                string trimmed = str.Trim();
                if (StringComparer.OrdinalIgnoreCase.Equals(trimmed, "true") ||
                    StringComparer.OrdinalIgnoreCase.Equals(trimmed, "yes"))
                    return true;

                if (StringComparer.OrdinalIgnoreCase.Equals(trimmed, "false") ||
                    StringComparer.OrdinalIgnoreCase.Equals(trimmed, "no"))
                    return false;

                return bool.Parse(str);
            }

            return Convert.ToBoolean(value);
        }

        public override bool TryGetValue(string keyword, out object value)
        {
            Keyword k;
            if (Keywords.TryGetValue(keyword, out k))
            {
                value = _values[k];
                return true;
            }

            value = null;
            return false;
        }

        public override bool ShouldSerialize(string keyword)
        {
            Keyword k;
            if (Keywords.TryGetValue(keyword, out k))
            {
                return _values[k] != DefaultValues[k];
            }
            return false;
        }

        public override ICollection Keys
        {
            get { return Enum.GetNames(typeof(Keyword)); }
        }

        public override ICollection Values
        {
            get { return _values.Values.ToArray(); }
        }

        public override bool ContainsKey(string keyword)
        {
            return Keywords.ContainsKey(keyword);
        }

        public override void Clear()
        {
            _values = new Dictionary<Keyword, object>(DefaultValues);
            base.Clear();
        }

        public override int Count
        {
            get { return _values.Count; }
        }

        public override bool IsFixedSize
        {
            get { return true; }
        }

        #region Nested type: Keyword

        private enum Keyword
        {
            Servers,
            Port,
            Username,
            Password,
            DiscoveryScope,
            ConnectionStrategy,
            CqlVersion,
            MaxDownTime,
            MinDownTime,
            MaxConnectionsPerNode,
            MaxConnections,
            NewConnectionTreshold,
            MaxConcurrentQueries,
            MaxConnectionIdleTime,
            MaxQueryRetries,
            LoggerFactory,
            LogLevel,
            UseBuffering,
            AllowCompression,
            CompressionTreshold
        }

        #endregion
    }
}