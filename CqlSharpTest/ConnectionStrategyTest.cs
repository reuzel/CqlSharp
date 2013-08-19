// CqlSharp - CqlSharp.Test
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

using System.Threading;
using CqlSharp.Config;
using CqlSharp.Network;
using CqlSharp.Network.Fakes;
using CqlSharp.Network.Partition;
using CqlSharp.Protocol;
using Microsoft.QualityTools.Testing.Fakes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace CqlSharp.Test
{
    [TestClass]
    public class ConnectionStrategyTest
    {
        [TestMethod]
        public async Task BalancedStrategyLowTreshold()
        {
            using (ShimsContext.Create())
            {
                //create cluster
                var config = new ClusterConfig { NewConnectionTreshold = 5 };

                var cluster = new Cluster(config);

                //create nodes
                var n = new Node(IPAddress.Parse("127.0.0.1"), cluster);
                var n2 = new Node(IPAddress.Parse("127.0.0.2"), cluster);
                var n3 = new Node(IPAddress.Parse("127.0.0.3"), cluster);
                var n4 = new Node(IPAddress.Parse("127.0.0.4"), cluster);
                var nodes = new Ring(new List<Node> { n, n2, n3, n4 }, "RandomPartitioner");

                ShimAllConnections();

                var logger = cluster.LoggerManager.GetLogger("BalancedStrategyLowTresholdTest");

                IConnectionStrategy strategy = new BalancedConnectionStrategy(nodes, config);

                const int nr = 8;

                for (int i = 0; i < nr; i++)
                {
                    Connection connection;

                    using (logger.ThreadBinding())
                        connection = strategy.GetOrCreateConnection(PartitionKey.None);

                    await connection.SendRequestAsync(new QueryFrame("", CqlConsistency.Any), logger, 10, false, CancellationToken.None);
                }

                Assert.AreEqual(nodes.Sum(nd => nd.ConnectionCount), nr);
                Assert.IsTrue(nodes.All(nd => nd.ConnectionCount == nr / 4));
            }
        }

        [TestMethod]
        public async Task BalancedStrategyManyRequestLowMaxConnections()
        {
            using (ShimsContext.Create())
            {
                //create cluster
                var config = new ClusterConfig { NewConnectionTreshold = 5, MaxConnections = 6 };

                var cluster = new Cluster(config);

                //create nodes
                var n1 = new Node(IPAddress.Parse("127.0.0.1"), cluster);
                var n2 = new Node(IPAddress.Parse("127.0.0.2"), cluster);
                var n3 = new Node(IPAddress.Parse("127.0.0.3"), cluster);
                var n4 = new Node(IPAddress.Parse("127.0.0.4"), cluster);
                var nodes = new Ring(new List<Node> { n1, n2, n3, n4 }, "RandomPartitioner");

                ShimAllConnections();

                var logger = cluster.LoggerManager.GetLogger("BalancedStrategyManyRequestLowMaxConnectionsTest");

                IConnectionStrategy strategy = new BalancedConnectionStrategy(nodes, config);

                const int nr = 80;

                for (int i = 0; i < nr; i++)
                {
                    Connection connection;

                    using (logger.ThreadBinding())
                        connection = strategy.GetOrCreateConnection(PartitionKey.None);

                    await connection.SendRequestAsync(new QueryFrame("", CqlConsistency.Any), logger, 10, false, CancellationToken.None);
                }

                Assert.AreEqual(6, nodes.Sum(nd => nd.ConnectionCount));
                Assert.IsTrue(nodes.All(n => n.Load == 80 * 10 / 4));
            }
        }

        [TestMethod]
        public async Task BalancedStrategyTestMedTreshold()
        {
            using (ShimsContext.Create())
            {
                //create cluster 
                var config = new ClusterConfig { NewConnectionTreshold = 20 };

                var cluster = new Cluster(config);

                //create nodes
                var n = new Node(IPAddress.Parse("127.0.0.1"), cluster);
                var n2 = new Node(IPAddress.Parse("127.0.0.2"), cluster);
                var n3 = new Node(IPAddress.Parse("127.0.0.3"), cluster);
                var n4 = new Node(IPAddress.Parse("127.0.0.4"), cluster);
                var nodes = new Ring(new List<Node> { n, n2, n3, n4 }, "RandomPartitioner");

                ShimAllConnections();

                var logger = cluster.LoggerManager.GetLogger("BalancedStrategyTestMedTresholdTest");

                IConnectionStrategy strategy = new BalancedConnectionStrategy(nodes, config);

                const int nr = 8;

                for (int i = 0; i < nr; i++)
                {
                    Connection connection;

                    using (logger.ThreadBinding())
                        connection = strategy.GetOrCreateConnection(PartitionKey.None);

                    await connection.SendRequestAsync(new QueryFrame("", CqlConsistency.Any), logger, 10, false, CancellationToken.None);
                }

                Assert.AreEqual(4, nodes.Sum(nd => nd.ConnectionCount));
                Assert.IsTrue(nodes.SelectMany(nd => nd).All(c => c.Load == 20));
            }
        }

        [TestMethod]
        public async Task BalancedStrategyTestHighTreshold()
        {
            using (ShimsContext.Create())
            {
                //create cluster 
                var config = new ClusterConfig { NewConnectionTreshold = 200 };

                var cluster = new Cluster(config);

                //create nodes
                var n1 = new Node(IPAddress.Parse("127.0.0.1"), cluster);
                var n2 = new Node(IPAddress.Parse("127.0.0.2"), cluster);
                var n3 = new Node(IPAddress.Parse("127.0.0.3"), cluster);
                var n4 = new Node(IPAddress.Parse("127.0.0.4"), cluster);
                var nodes = new Ring(new List<Node> { n1, n2, n3, n4 }, "RandomPartitioner");

                ShimAllConnections();

                var logger = cluster.LoggerManager.GetLogger("BalancedStrategyTestHighTresholdTest");

                IConnectionStrategy strategy = new BalancedConnectionStrategy(nodes, config);

                const int nr = 8;

                for (int i = 0; i < nr; i++)
                {
                    Connection connection;

                    using (logger.ThreadBinding())
                        connection = strategy.GetOrCreateConnection(PartitionKey.None);

                    await connection.SendRequestAsync(new QueryFrame("", CqlConsistency.Any), logger, 10, false, CancellationToken.None);
                }

                Assert.AreEqual(1, nodes.Sum(nd => nd.ConnectionCount));
                //Assert.IsTrue(nodes.SelectMany(nd => nd).All(c => c.Load == 20));
            }
        }

        [TestMethod]
        public async Task BalancedStrategyTestMaxConnections()
        {
            using (ShimsContext.Create())
            {
                //create cluster
                var config = new ClusterConfig { NewConnectionTreshold = 5, MaxConnections = 6 };

                var cluster = new Cluster(config);

                //create nodes
                var n = new Node(IPAddress.Parse("127.0.0.1"), cluster);
                var n2 = new Node(IPAddress.Parse("127.0.0.2"), cluster);
                var n3 = new Node(IPAddress.Parse("127.0.0.3"), cluster);
                var n4 = new Node(IPAddress.Parse("127.0.0.4"), cluster);
                var nodes = new Ring(new List<Node> { n, n2, n3, n4 }, "RandomPartitioner");

                ShimAllConnections();

                var logger = cluster.LoggerManager.GetLogger("BalancedStrategyTestMaxConnections");

                IConnectionStrategy strategy = new BalancedConnectionStrategy(nodes, config);

                const int nr = 8;

                for (int i = 0; i < nr; i++)
                {
                    Connection connection;

                    using (logger.ThreadBinding())
                        connection = strategy.GetOrCreateConnection(PartitionKey.None);

                    await connection.SendRequestAsync(new QueryFrame("", CqlConsistency.Any), logger, 10, false, CancellationToken.None);
                }

                Assert.AreEqual(nodes.Sum(nd => nd.ConnectionCount), 6);
                Assert.IsTrue(nodes.All(nd => nd.ConnectionCount == 1 || nd.ConnectionCount == 2));
            }
        }


        [TestMethod]
        public async Task BalancedStrategyFewRequests()
        {
            using (ShimsContext.Create())
            {
                //create cluster 
                var config = new ClusterConfig { NewConnectionTreshold = 20 };

                var cluster = new Cluster(config);

                //create nodes
                var n = new Node(IPAddress.Parse("127.0.0.1"), cluster);
                var n2 = new Node(IPAddress.Parse("127.0.0.2"), cluster);
                var n3 = new Node(IPAddress.Parse("127.0.0.3"), cluster);
                var n4 = new Node(IPAddress.Parse("127.0.0.4"), cluster);
                var nodes = new Ring(new List<Node> { n, n2, n3, n4 }, "RandomPartitioner");

                ShimAllConnections();

                var logger = cluster.LoggerManager.GetLogger("BalancedStrategyFewRequestsTest");

                IConnectionStrategy strategy = new BalancedConnectionStrategy(nodes, config);

                const int nr = 8;

                for (int i = 0; i < nr; i++)
                {
                    Connection connection;

                    using (logger.ThreadBinding())
                        connection = strategy.GetOrCreateConnection(PartitionKey.None);

                    await connection.SendRequestAsync(new QueryFrame("", CqlConsistency.Any), logger, 10, false, CancellationToken.None);
                }

                Assert.AreEqual(4, nodes.Sum(nd => nd.ConnectionCount));
                Assert.IsTrue(nodes.All(nd => nd.ConnectionCount == 1));
                Assert.IsTrue(nodes.SelectMany(nd => nd).All(c => c.Load == 20));
            }
        }

        [TestMethod]
        public async Task BalancedStrategyManyRequests()
        {
            using (ShimsContext.Create())
            {
                //create cluster 
                var config = new ClusterConfig { NewConnectionTreshold = 20 };

                var cluster = new Cluster(config);

                //create nodes
                var n = new Node(IPAddress.Parse("127.0.0.1"), cluster);
                var n2 = new Node(IPAddress.Parse("127.0.0.2"), cluster);
                var n3 = new Node(IPAddress.Parse("127.0.0.3"), cluster);
                var n4 = new Node(IPAddress.Parse("127.0.0.4"), cluster);
                var nodes = new Ring(new List<Node> { n, n2, n3, n4 }, "RandomPartitioner");

                ShimAllConnections();


                var logger = cluster.LoggerManager.GetLogger("BalancedStrategyManyRequestsTest");

                IConnectionStrategy strategy = new BalancedConnectionStrategy(nodes, config);

                const int nr = 80;

                for (int i = 0; i < nr; i++)
                {
                    Connection connection;

                    using (logger.ThreadBinding())
                        connection = strategy.GetOrCreateConnection(PartitionKey.None);

                    await connection.SendRequestAsync(new QueryFrame("", CqlConsistency.Any), logger, 10, false, CancellationToken.None);
                }

                Assert.AreEqual(nodes.Sum(nd => nd.ConnectionCount), 8);
                Assert.IsTrue(nodes.All(nd => nd.ConnectionCount == 2));
                Assert.IsTrue(nodes.SelectMany(nd => nd).All(c => c.Load == (80 * 10) / 4 / 2));
            }
        }

        private static void ShimAllConnections()
        {
            //shim connections to avoid network connections...
            ShimConnection.ConstructorIPAddressClusterInt32 = (conn, address, conf, nr) =>
                                                                  {
                                                                      //wrap the new connection in a shim
                                                                      var connection = new ShimConnection(conn);
                                                                      int connLoad = 0;
                                                                      EventHandler<LoadChangeEvent> nodeHandler = null;

                                                                      //replace any IO inducing methods
                                                                      connection.OpenAsyncLogger =
                                                                          log => Task.FromResult(true);


                                                                      // ReSharper disable AccessToModifiedClosure
                                                                      connection.SendRequestAsyncFrameLoggerInt32BooleanCancellationToken
                                                                          =
                                                                          (frame, log, load, connecting, token) =>
                                                                          {
                                                                              //update connection load
                                                                              connLoad += load;
                                                                              //call load change event handler
                                                                              if (nodeHandler != null)
                                                                                  nodeHandler(connection,
                                                                                           new LoadChangeEvent { LoadDelta = load });
                                                                              //done
                                                                              return
                                                                                  Task.FromResult(
                                                                                      (Frame)
                                                                                      new ResultFrame { Stream = frame.Stream });
                                                                          };
                                                                      // ReSharper restore AccessToModifiedClosure

                                                                      //intercept load changed handlers
                                                                      connection.
                                                                          OnLoadChangeAddEventHandlerOfLoadChangeEvent
                                                                          = handler => { nodeHandler += handler; };

                                                                      //return proper load values
                                                                      connection.LoadGet = () => connLoad;

                                                                      //set some default properties
                                                                      connection.IsConnectedGet = () => true;
                                                                      connection.IsIdleGet = () => false;
                                                                  };
        }
    }
}