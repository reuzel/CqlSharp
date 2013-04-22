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
using System.Configuration;
using System.Text;

namespace CqlSharp.Config
{
    /// <summary>
    ///   Specifies a Cassandra configuration
    /// </summary>
    public class ClusterConfig
    {
        private const int DefaultPort = 9042;
        private const DiscoveryScope DefaultDiscoveryScope = DiscoveryScope.None;
        private const ConnectionStrategy DefaultConnectionStrategy = ConnectionStrategy.Balanced;
        private const string DefaultCqlVersion = "3.0.0";
        private const int DefaultMaxDownTime = 60*60*1000; //max down for 1 hour
        private const int DefaultMinDownTime = 500; //min down time for .5 second
        private const int DefaultMaxConnectionsPerNode = 2;
        private const int DefaultMaxConnections = -1; //no total max
        private const int DefaultNewConnectionTreshold = 10; //new connection when 10 parallel queries on one connection
        private const int DefaultMaxConcurrentQueries = -1;
        private static readonly char[] PartSeperator = new[] {';'};
        private static readonly char[] ValueSeperator = new[] {'='};
        private static readonly TimeSpan DefaultMaxConnectionIdleTime = TimeSpan.FromSeconds(10);

        /// <summary>
        ///   Initializes a new instance of the <see cref="ClusterConfig" /> class.
        /// </summary>
        public ClusterConfig()
        {
            Nodes = new List<string>();
            Port = DefaultPort;
            DiscoveryScope = DefaultDiscoveryScope;
            ConnectionStrategy = DefaultConnectionStrategy;
            CqlVersion = DefaultCqlVersion;
            MaxDownTime = DefaultMaxDownTime;
            MinDownTime = DefaultMinDownTime;
            MaxConnectionsPerNode = DefaultMaxConnectionsPerNode;
            MaxConnections = DefaultMaxConnections;
            NewConnectionTreshold = DefaultNewConnectionTreshold;
            MaxConcurrentQueries = DefaultMaxConcurrentQueries;
            MaxConnectionIdleTime = DefaultMaxConnectionIdleTime;
        }


        /// <summary>
        ///   Initializes a new instance of the <see cref="ClusterConfig" /> class.
        /// </summary>
        /// <param name="connectionString"> The connection string, or the name of the connectionstring in the connectionstring file </param>
        public ClusterConfig(string connectionString)
            : this()
        {
            ConnectionStringSettings fromConfig = ConfigurationManager.ConnectionStrings[connectionString];
            string toParse = fromConfig == null ? connectionString : fromConfig.ConnectionString;
            Parse(toParse);
        }


        /// <summary>
        ///   Gets or sets the port Cassandra is configured to listen to Binary Protocol.
        /// </summary>
        /// <value> The binary protocol port as specified in the Cassandra configuration (yaml). Default 9042 </value>
        public int Port { get; set; }

        /// <summary>
        ///   Gets or sets a comma seperated list node addresses or ip's pointing to nodes in Cassandra cluster.
        /// </summary>
        /// <value> The IP-addresses or DNS names of the nodes. </value>
        public List<string> Nodes { get; set; }

        /// <summary>
        ///   Gets or sets the discovery scope. Used to enlarge the list of nodes to which connections can be made
        /// </summary>
        /// <value> The discovery scope. Default: None </value>
        public DiscoveryScope DiscoveryScope { get; set; }


        /// <summary>
        ///   Gets or sets the connection strategy used to connect to nodes.
        /// </summary>
        /// <value> The connection strategy. Default: Balanced </value>
        public ConnectionStrategy ConnectionStrategy { get; set; }

        /// <summary>
        ///   Gets or sets the username used to connect to the Cassandra cluster
        /// </summary>
        /// <value> The username. Default: null </value>
        public string Username { get; set; }


        /// <summary>
        ///   Gets or sets the password.
        /// </summary>
        /// <value> The password. Default: null </value>
        public string Password { get; set; }

        /// <summary>
        ///   Gets or sets the CQL version.
        /// </summary>
        /// <value> The CQL version. Default: 3.0.0 </value>
        public string CqlVersion { get; set; }

        /// <summary>
        ///   Gets or sets the max down time of a cassandra node, when connections failed. When a node is marked down,
        ///   no new connection attempts are made.
        /// </summary>
        /// <value> The max down time in ms. Default: 1 hour </value>
        public int MaxDownTime { get; set; }


        /// <summary>
        ///   Gets or sets the min down time of a node when a connection failed.
        /// </summary>
        /// <value> The min down time in ms. Default 500ms </value>
        public int MinDownTime { get; set; }

        /// <summary>
        ///   Gets or sets the max connections per node.
        /// </summary>
        /// <value> The max connections per node. Default 2 </value>
        public int MaxConnectionsPerNode { get; set; }


        /// <summary>
        ///   Gets or sets the max connections to the cluster.
        /// </summary>
        /// <value> The max connections. When 0 or negative, no maximum. Default: -1 </value>
        public int MaxConnections { get; set; }

        /// <summary>
        ///   Gets or sets the new connection treshold. This threshold defines when a new connection is created
        ///   when other connections are "full"
        /// </summary>
        /// <value> The new connection treshold. Default: 10 </value>
        public int NewConnectionTreshold { get; set; }

        /// <summary>
        ///   Gets or sets the max concurrent queries. Threads attempting to execute a query (whether async or not) will be blocked until
        ///   the number of active queries drops below this number.
        /// </summary>
        /// <value> The max concurrent queries. if 0 or negative, the max will be calculated by the number of found nodes in the cluster * MaxConnectionsPerNode * 2. </value>
        public int MaxConcurrentQueries { get; set; }

        /// <summary>
        ///   Gets or sets the max connection idle time. Any connection that did not perform a query within this timespan is elligable to be closed.
        /// </summary>
        /// <value> The max connection idle time in seconds. Default 10 seconds. </value>
        public TimeSpan MaxConnectionIdleTime { get; set; }

        /// <summary>
        ///   Parses the specified connectionstring.
        /// </summary>
        /// <param name="connectionstring"> The connectionstring. </param>
        /// <exception cref="CqlException">Configuration error: Could not split the configuration element in a key and value:  + part</exception>
        private void Parse(string connectionstring)
        {
            string[] parts = connectionstring.Split(PartSeperator, StringSplitOptions.RemoveEmptyEntries);
            foreach (string part in parts)
            {
                string[] kv = part.Split(ValueSeperator, StringSplitOptions.RemoveEmptyEntries);
                if (kv.Length != 2)
                    throw new CqlException(
                        "Configuration error: Could not split the configuration element in a key and value: " + part);

                string key = kv[0].Trim().Trim(new[] {'\'', '"'}).ToLower();
                string value = kv[1].Trim().Trim(new[] {'\'', '"'});

                switch (key)
                {
                    case "server":
                    case "servers":
                    case "node":
                    case "nodes":
                        Nodes = new List<string>(value.Split(','));
                        break;
                    case "port":
                        Port = int.Parse(value);
                        break;
                    case "discovery scope":
                    case "discoveryscope":
                        DiscoveryScope = (DiscoveryScope) Enum.Parse(typeof (DiscoveryScope), value, true);
                        break;
                    case "connection strategy":
                    case "connectionstrategy":
                        ConnectionStrategy = (ConnectionStrategy) Enum.Parse(typeof (ConnectionStrategy), value, true);
                        break;
                    case "cql":
                    case "version":
                    case "cql version":
                    case "cqlversion":
                        CqlVersion = value;
                        break;
                    case "user name":
                    case "user id":
                    case "username":
                    case "user":
                        Username = value;
                        break;
                    case "password":
                        Password = value;
                        break;
                    case "max downtime":
                    case "maxdowntime":
                        MaxDownTime = int.Parse(value);
                        break;
                    case "min downtime":
                    case "mindowntime":
                        MinDownTime = int.Parse(value);
                        break;
                    case "max connections":
                    case "maxconnections":
                        MaxConnections = int.Parse(value);
                        break;
                    case "connections per node":
                    case "max connections per node":
                    case "maxconnectionspernode":
                    case "connectionspernode":
                    case "maxpernode":
                    case "max per node":
                        MaxConnectionsPerNode = int.Parse(value);
                        break;
                    case "newconnectiontreshold":
                    case "new connection treshold":
                    case "treshold":
                    case "connection treshold":
                        NewConnectionTreshold = int.Parse(value);
                        break;
                    case "maxconcurrentqueries":
                    case "max concurrent queries":
                    case "max concurrent":
                    case "concurrent queries":
                    case "concurrentqueries":
                    case "max queries":
                    case "maxqueries":
                    case "throttle":
                        MaxConcurrentQueries = int.Parse(value);
                        break;
                    case "maxconnectionidletime":
                    case "max connection idle time":
                    case "connectionidletime":
                    case "connection idle time":
                    case "maxidletime":
                    case "max idle time":
                        MaxConnectionIdleTime = TimeSpan.FromSeconds(int.Parse(value));
                        break;
                    default:
                        throw new CqlException("Config error: unknown configuration property: " + key);
                }
            }
        }

        /// <summary>
        ///   Returns a <see cref="System.String" /> that represents this configuration. Usable as Connection String.
        /// </summary>
        /// <returns> A <see cref="System.String" /> that represents this instance. </returns>
        public override string ToString()
        {
            var builder = new StringBuilder();
            builder.Append("Servers=");
            builder.Append(string.Join(",", Nodes));
            builder.Append(";");

            if (Port != DefaultPort)
            {
                builder.Append("Port=");
                builder.Append(Port);
                builder.Append(";");
            }

            if (Username != null)
            {
                builder.Append("User=");
                builder.Append(Username);
                builder.Append(";");
            }

            if (Password != null)
            {
                builder.Append("Password=");
                builder.Append(Password);
                builder.Append(";");
            }

            if (!CqlVersion.Equals(DefaultCqlVersion))
            {
                builder.Append("CqlVersion=");
                builder.Append(CqlVersion);
                builder.Append(";");
            }

            if (!DiscoveryScope.Equals(DefaultDiscoveryScope))
            {
                builder.Append("DiscoveryScope=");
                builder.Append(DiscoveryScope);
                builder.Append(";");
            }

            if (!ConnectionStrategy.Equals(DefaultConnectionStrategy))
            {
                builder.Append("ConnectionStrategy=");
                builder.Append(ConnectionStrategy);
                builder.Append(";");
            }

            if (!NewConnectionTreshold.Equals(DefaultNewConnectionTreshold))
            {
                builder.Append("NewConnectionTreshold=");
                builder.Append(NewConnectionTreshold);
                builder.Append(";");
            }

            if (!MaxDownTime.Equals(DefaultMaxDownTime))
            {
                builder.Append("MaxDownTime=");
                builder.Append(MaxDownTime);
                builder.Append(";");
            }

            if (!MinDownTime.Equals(DefaultMinDownTime))
            {
                builder.Append("MinDownTime=");
                builder.Append(MinDownTime);
                builder.Append(";");
            }

            if (MaxConnections > 0)
            {
                builder.Append("MaxConnections=");
                builder.Append(MaxConnections);
                builder.Append(";");
            }

            if (!MaxConnectionsPerNode.Equals(DefaultMaxConnectionsPerNode))
            {
                builder.Append("MaxConnectionsPerNode=");
                builder.Append(MaxConnectionsPerNode);
                builder.Append(";");
            }

            if (MaxConcurrentQueries > 0)
            {
                builder.Append("MaxConcurrentQueries=");
                builder.Append(MaxConcurrentQueries);
                builder.Append(";");
            }

            if (MaxConnectionIdleTime != DefaultMaxConnectionIdleTime)
            {
                builder.Append("MaxConnectionIdleTime=");
                builder.Append(MaxConnectionIdleTime.TotalSeconds);
                builder.Append(";");
            }

            return builder.ToString();
        }
    }
}