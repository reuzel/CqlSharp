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
using CqlSharp.Protocol;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

//suppressing warnings about unobserved tasks
#pragma warning disable 168

namespace CqlSharp.Network
{
    /// <summary>
    ///   A single connection to a Cassandra node.
    /// </summary>
    internal class Connection
    {
        private readonly IPAddress _address;

        private readonly Queue<sbyte> _availableQueryIds;
        private readonly Cluster _cluster;
        private readonly SemaphoreSlim _frameSubmitLock;
        private readonly IDictionary<sbyte, TaskCompletionSource<Frame>> _openRequests;

        private readonly SemaphoreSlim _writeLock;
        private int _activeRequests;
        private TcpClient _client;
        private int _connectionState; //0=disconnected, 1=connected, 2=failed/closed
        private long _lastActivity;
        private int _load;
        private Stream _readStream;
        private Stream _writeStream;

        private readonly object _syncLock = new object();
        private volatile Task _connectTask;
        private readonly int _nr;
        private bool _allowCompression;


        /// <summary>
        /// Initializes a new instance of the <see cref="Connection" /> class.
        /// </summary>
        /// <param name="address">The address.</param>
        /// <param name="cluster">The cluster.</param>
        /// <param name="nr">The connection nr.</param>
        public Connection(IPAddress address, Cluster cluster, int nr)
        {
            _address = address;
            _cluster = cluster;
            _nr = nr;

            //setup support for multiple queries
            _availableQueryIds = new Queue<sbyte>();
            for (sbyte i = 0; i < sbyte.MaxValue; i++)
                _availableQueryIds.Enqueue(i);

            _openRequests = new Dictionary<sbyte, TaskCompletionSource<Frame>>();

            //setup locks
            _writeLock = new SemaphoreSlim(1);
            _frameSubmitLock = new SemaphoreSlim(sbyte.MaxValue);

            //setup state
            _activeRequests = 0;
            _load = 0;
            _connectionState = 0;
            _lastActivity = DateTime.Now.Ticks;

            Logger.Current.LogVerbose("{0} created", this);
        }

        /// <summary>
        ///   Gets the load.
        /// </summary>
        /// <value> The load. </value>
        public int Load
        {
            get { return _load; }
        }

        /// <summary>
        ///   Gets a value indicating whether this instance is idle. An connection is idle if it
        ///   has failed or was disconnected, or when the load is zero and the last activity is older
        ///   than the configured MaxConnectionIdleTime.
        /// </summary>
        /// <value> <c>true</c> if this instance is idle; otherwise, <c>false</c> . </value>
        public bool IsIdle
        {
            get
            {
                return _connectionState == 2 ||
                       (_activeRequests == 0 &&
                        (DateTime.Now.Ticks - Interlocked.Read(ref _lastActivity)) > _cluster.Config.MaxConnectionIdleTime.Ticks);
            }
        }

        /// <summary>
        ///   Gets the number of active requests.
        /// </summary>
        /// <value> The active requests. </value>
        public int ActiveRequests
        {
            get { return _activeRequests; }
        }

        /// <summary>
        ///   Gets a value indicating whether this instance is connected.
        /// </summary>
        /// <value> <c>true</c> if this instance is connected; otherwise, <c>false</c> . </value>
        public bool IsConnected
        {
            get { return _connectionState == 0 || _connectionState == 1; }
        }

        /// <summary>
        ///   Gets the address.
        /// </summary>
        /// <value> The address. </value>
        public IPAddress Address
        {
            get { return _address; }
        }

        /// <summary>
        ///   Occurs when [on connection change].
        /// </summary>
        public event EventHandler<ConnectionChangeEvent> OnConnectionChange;

        /// <summary>
        ///   Occurs when [on load change].
        /// </summary>
        public event EventHandler<LoadChangeEvent> OnLoadChange;

        /// <summary>
        /// Occurs when [on cluster change].
        /// </summary>
        public event EventHandler<ClusterChangedEvent> OnClusterChange;

        /// <summary>
        /// Updates the load of this connection, and will trigger a corresponding event
        /// </summary>
        /// <param name="load">The load.</param>
        /// <param name="logger">The logger.</param>
        private void UpdateLoad(int load, Logger logger)
        {
            var newLoad = Interlocked.Add(ref _load, load);
            Interlocked.Exchange(ref _lastActivity, DateTime.Now.Ticks);

            EventHandler<LoadChangeEvent> handler = OnLoadChange;
            if (handler != null) handler(this, new LoadChangeEvent { LoadDelta = load });

            logger.LogVerbose("{0} has now a load of {1}", this, newLoad);
        }

        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        ///   Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"> <c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources. </param>
        /// <param name="error"> The error being the reason for the connection disposal </param>
        protected void Dispose(bool disposing, Exception error = null)
        {
            if (Interlocked.Exchange(ref _connectionState, 2) != 2)
            {
                if (disposing)
                {
                    _frameSubmitLock.Dispose();
                    _writeLock.Dispose();

                    if (_client != null)
                    {
                        if (error != null)
                        {
                            _client.LingerState = new LingerOption(true, 0); //TCP reset
                            Logger.Current.LogWarning("Resetting {0} because of error {1}", this, error);
                        }

                        _client.Close();
                        _client = null;

                        Logger.Current.LogInfo("Connection to {0} closed", Address);
                    }

                    if (OnConnectionChange != null)
                        OnConnectionChange(this, new ConnectionChangeEvent { Exception = error, Connected = false });
                }

                OnConnectionChange = null;
                OnLoadChange = null;
            }
        }

        /// <summary>
        ///   Finalizes an instance of the <see cref="Connection" /> class.
        /// </summary>
        ~Connection()
        {
            Dispose(false);
        }


        /// <summary>
        /// Opens the connection. The actual open sequence will be executed at most once.
        /// </summary>
        /// <returns>Task that represents the open procedure for this connection</returns>
        private Task OpenAsync(Logger logger)
        {
            if (_connectTask == null)
            {
                lock (_syncLock)
                {
                    if (_connectTask == null)
                    {
                        _connectTask = OpenAsyncInternal(logger);
                    }
                }
            }

            return _connectTask;
        }

        /// <summary>
        ///   Opens the connection
        /// </summary>
        private async Task OpenAsyncInternal(Logger logger)
        {
            //switch state to connecting if not done so
            int state = Interlocked.CompareExchange(ref _connectionState, 1, 0);

            if (state == 1)
                return;

            if (state == 2)
                throw new ObjectDisposedException("Connection disposed before opening!");

            try
            {
                //create TCP connection
                _client = new TcpClient();
                await _client.ConnectAsync(_address, _cluster.Config.Port).ConfigureAwait(false);
                _writeStream = _client.GetStream();
                _readStream = _client.GetStream();

                logger.LogVerbose("TCP connection to {0} is opened", Address);

                //start readloop
                StartReadingAsync();

                //get compression option
                _allowCompression = false; //assume false unless
                if (_cluster.Config.AllowCompression)
                {
                    //check wether compression is supported by getting compression options from server
                    var options = new OptionsFrame();
                    var supported =
                        await SendRequestAsyncInternal(options, logger, 1, true, CancellationToken.None).ConfigureAwait(false) as SupportedFrame;

                    if (supported == null)
                        throw new ProtocolException(0, "Expected Supported frame not received");

                    IList<string> compressionOptions;
                    //check if options contain compression
                    if (supported.SupportedOptions.TryGetValue("COMPRESSION", out compressionOptions))
                    {
                        //check wether snappy is supported
                        _allowCompression = compressionOptions.Contains("snappy");
                    }

                    //dispose supported frame
                    supported.Dispose();
                }

                //submit startup frame
                var startup = new StartupFrame(_cluster.Config.CqlVersion);
                if (_allowCompression)
                {
                    logger.LogVerbose("Enabling Snappy Compression.");
                    startup.Options["COMPRESSION"] = "snappy";
                }

                Frame response = await SendRequestAsyncInternal(startup, logger, 1, true, CancellationToken.None).ConfigureAwait(false);

                //authenticate if required
                var auth = response as AuthenticateFrame;
                if (auth != null)
                {
                    logger.LogVerbose("Authentication requested, attempting to provide credentials", Address);

                    //check if _username is actually set
                    if (_cluster.Config.Username == null || _cluster.Config.Password == null)
                        throw new UnauthorizedException("No credentials provided");

                    //dispose AuthenticateFrame
                    response.Dispose();

                    var cred = new CredentialsFrame(_cluster.Config.Username, _cluster.Config.Password);
                    response = await SendRequestAsyncInternal(cred, logger, 1, true, CancellationToken.None).ConfigureAwait(false);
                }

                //check if ready
                if (!(response is ReadyFrame))
                    throw new ProtocolException(0, "Expected Ready frame not received");

                //dispose ready frame
                response.Dispose();

                using (logger.ThreadBinding())
                {
                    if (OnConnectionChange != null)
                        OnConnectionChange(this, new ConnectionChangeEvent { Connected = true });
                }

                logger.LogInfo("{0} is opened and ready for use", this);
            }
            catch (Exception ex)
            {
                using (logger.ThreadBinding())
                {
                    Dispose(true, ex);
                    throw;
                }
            }
        }

        /// <summary>
        /// Submits a frame, and waits until response is received
        /// </summary>
        /// <param name="frame">The frame to send.</param>
        /// <param name="logger">logger to write progress to</param>
        /// <param name="load">the load indication of the request. Used for balancing queries over nodes and connections</param>
        /// <param name="isConnecting">indicates if this request is send as part of connection setup protocol</param>
        /// <param name="token">The token.</param>
        /// <returns></returns>
        /// <exception cref="System.IO.IOException">Not connected</exception>
        internal Task<Frame> SendRequestAsync(Frame frame, Logger logger, int load, bool isConnecting, CancellationToken token)
        {
            return token.CanBeCanceled ? SendCancellableRequestAsync(frame, logger, load, isConnecting, token) : SendRequestAsyncInternal(frame, logger, load, isConnecting, token);
        }

        /// <summary>
        /// Sends the cancellable request async. Adds a cancellation wrapper around SendRequestInternal
        /// </summary>
        /// <param name="frame">The frame.</param>
        /// <param name="logger">The logger.</param>
        /// <param name="load">The load.</param>
        /// <param name="isConnecting">if set to <c>true</c> [is connecting].</param>
        /// <param name="token">The token.</param>
        /// <returns></returns>
        /// <exception cref="System.OperationCanceledException"></exception>
        private async Task<Frame> SendCancellableRequestAsync(Frame frame, Logger logger, int load, bool isConnecting, CancellationToken token)
        {
            var task = SendRequestAsyncInternal(frame, logger, load, isConnecting, token);
            var cancelTask = new TaskCompletionSource<bool>();
            using (token.Register(s => ((TaskCompletionSource<bool>)s).TrySetResult(true), cancelTask))
            {
                //wait for either sendTask or cancellation task to complete
                if (task != await Task.WhenAny(task, cancelTask.Task).ConfigureAwait(false))
                {
                    //ignore/log any exception of the handled task
                    var logError = task.ContinueWith((sendTask, log) =>
                                          {
                                              if (sendTask.Exception != null)
                                              {
                                                  var logger1 = (Logger)log;
                                                  logger1.LogWarning("Cancelled query threw exception: {0}",
                                                                     sendTask.Exception.InnerException);
                                              }
                                          }, logger, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);

                    //get this request cancelled
                    throw new OperationCanceledException(token);
                }
            }
            return await task;
        }


        /// <summary>
        /// Sends the request async internal. Cancellation supported until request is send, after which answer must be handled
        /// to avoid connection corruption.
        /// </summary>
        /// <param name="frame">The frame.</param>
        /// <param name="logger">The logger.</param>
        /// <param name="load">The load.</param>
        /// <param name="isConnecting">if set to <c>true</c> [is connecting].</param>
        /// <param name="token">The token.</param>
        /// <returns></returns>
        /// <exception cref="System.IO.IOException">Not connected</exception>
        private async Task<Frame> SendRequestAsyncInternal(Frame frame, Logger logger, int load, bool isConnecting, CancellationToken token)
        {
            try
            {
                //make sure we're already connected
                if (!isConnecting)
                    await OpenAsync(logger).ConfigureAwait(false);

                //make sure we are connected
                if (!IsConnected)
                    throw new IOException("Not connected");

                //count the operation
                Interlocked.Increment(ref _activeRequests);

                //increase the load
                UpdateLoad(load, logger);

                logger.LogVerbose("Waiting for connection lock on {0}...", this);

                //wait until allowed to submit a frame
                await _frameSubmitLock.WaitAsync(token).ConfigureAwait(false);

                //get a task that gets completed when a response is received
                var waitTask = new TaskCompletionSource<Frame>();

                //get a stream id, and store wait task under that id
                sbyte id;
                lock (_availableQueryIds)
                {
                    id = _availableQueryIds.Dequeue();
                    _openRequests.Add(id, waitTask);
                }

                try
                {
                    //send frame
                    frame.Stream = id;

                    //serialize frame outside lock
                    Stream frameBytes = frame.GetFrameBytes(_allowCompression && !isConnecting, _cluster.Config.CompressionTreshold);

                    await _writeLock.WaitAsync(token).ConfigureAwait(false);
                    try
                    {
                        //final check to make sure we're connected
                        if (_connectionState != 1)
                            throw new IOException("Not connected");

                        logger.LogVerbose("Sending {0} Frame with Id {1}, to {2}", frame.OpCode, id, this);

                        //write frame to stream, don't use cancelToken to prevent half-written frames
                        await frameBytes.CopyToAsync(_writeStream).ConfigureAwait(false);
                    }
                    finally
                    {
                        _writeLock.Release();
                        frameBytes.Dispose();
                    }

                    //wait until response is received
                    Frame response = await waitTask.Task.ConfigureAwait(false);

                    logger.LogVerbose("{0} response for frame with Id {1} received from {2}", response.OpCode, id, Address);

                    //throw error if result is an error
                    var error = response as ErrorFrame;
                    if (error != null)
                    {
                        //dispose error frame and throw the exception
                        error.Dispose();
                        throw error.Exception;
                    }

                    //dispose frame, when cancellation requested
                    if (token.IsCancellationRequested)
                    {
                        response.Dispose();
                        throw new OperationCanceledException(token);
                    }

                    //return response
                    return response;
                }
                finally
                {
                    //return request slot to the pool
                    lock (_availableQueryIds)
                    {
                        _openRequests.Remove(id);
                        _availableQueryIds.Enqueue(id);
                    }

                    //allow another frame to be send
                    _frameSubmitLock.Release();

                    //reduce load, we are done
                    Interlocked.Decrement(ref _activeRequests);
                    UpdateLoad(-load, logger);
                }
            }
            catch (TaskCanceledException)
            {
                throw;
            }
            catch (ProtocolException pex)
            {
                switch (pex.Code)
                {
                    case ErrorCode.IsBootstrapping:
                    case ErrorCode.Overloaded:

                        using (logger.ThreadBinding())
                        {
                            //IO or node status related error, dispose this connection
                            Dispose(true, pex);
                            throw;
                        }

                    default:
                        //some other Cql error (syntax ok?), simply rethrow
                        throw;
                }
            }
            catch (Exception ex)
            {
                using (logger.ThreadBinding())
                {
                    //connection collapsed, dispose this connection
                    Dispose(true, ex);
                    throw;
                }
            }
        }

        /// <summary>
        ///   Starts a readloop
        /// </summary>
        private async void StartReadingAsync()
        {
            var logger = _cluster.LoggerManager.GetLogger("CqlSharp.Connection.ReadLoop");

            //while connected do
            while (_connectionState == 1)
            {
                try
                {
                    logger.LogVerbose("Waiting for new frame to arrive on {0}", this);

                    //read next frame from stream
                    Frame frame = await Frame.FromStream(_readStream).ConfigureAwait(false);

                    //check if frame is event
                    if (frame.Stream == -1)
                    {
                        var eventFrame = frame as EventFrame;
                        if (eventFrame == null)
                            throw new ProtocolException(ErrorCode.Protocol, "A frame is received with StreamId -1, while it is not an EventFrame");

                        logger.LogVerbose("Event frame received on {0}", this);

                        //run the event logic in its own task, making sure it does not delay further reading
                        Task eventTask = Task.Run(() => ProcessEvent(eventFrame));
                        continue;
                    }

                    //get the request waiting on this response
                    TaskCompletionSource<Frame> openRequest;
                    lock (_availableQueryIds)
                    {
                        if (!_openRequests.TryGetValue(frame.Stream, out openRequest))
                        {
                            if (frame.OpCode == FrameOpcode.Error)
                            {
                                var error = (ErrorFrame)frame;
                                throw error.Exception;
                            }

                            throw new ProtocolException(ErrorCode.Protocol, "Frame with unknown Stream received");
                        }
                    }

                    //signal frame received. As a new task, because task
                    //completions may be continued synchronously, potentially
                    //leading to deadlocks when the continuation sends another request
                    //on this connection.
                    Task continueOpenRequestTask = Task.Run(() => openRequest.TrySetResult(frame));

                    logger.LogVerbose("Waiting for frame content to be read from {0}", this);

                    //wait until all frame data is read (especially important for queries and results)
                    await frame.WaitOnBodyRead().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    using (logger.ThreadBinding())
                    {
                        //error occured during read operaton, assume connection is dead, switch state
                        Dispose(true, ex);
                    }
                }
            }

            //we stopped reading, fail all other open requests
            List<TaskCompletionSource<Frame>> unfinishedRequests;
            lock (_availableQueryIds)
            {
                unfinishedRequests = new List<TaskCompletionSource<Frame>>(_openRequests.Values);
                if (unfinishedRequests.Count > 0)
                    logger.LogWarning("{0} closed, throwing connection closed error for {1} queries", this, unfinishedRequests.Count);
            }

            //iterate over all open request and finish them with an exception
            var closedException = new IOException("Connection closed before receiving a result.");
            foreach (var req in unfinishedRequests)
            {
                req.TrySetException(closedException);
            }
        }

        /// <summary>
        ///   Registers for cluster changes.
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="System.InvalidOperationException">Must be connected before Registration can take place</exception>
        /// <exception cref="CqlException">Could not register for cluster changes!</exception>
        public async Task RegisterForClusterChangesAsync(Logger logger)
        {
            var registerframe = new RegisterFrame(new List<string> { "TOPOLOGY_CHANGE", "STATUS_CHANGE" });
            Frame result = await SendRequestAsync(registerframe, logger, 1, false, CancellationToken.None).ConfigureAwait(false);

            if (!(result is ReadyFrame))
                throw new CqlException("Could not register for cluster changes!");

            //increase request count to prevent connection to go in Idle state
            Interlocked.Increment(ref _activeRequests);
        }

        /// <summary>
        ///   Processes the event frame.
        /// </summary>
        /// <param name="frame"> The frame. </param>
        private void ProcessEvent(EventFrame frame)
        {
            if (frame.EventType.Equals("TOPOLOGY_CHANGE", StringComparison.InvariantCultureIgnoreCase) ||
                frame.EventType.Equals("STATUS_CHANGE", StringComparison.InvariantCultureIgnoreCase))
            {
                ClusterChange change;

                switch (frame.Change.ToLower())
                {
                    case "up":
                        change = ClusterChange.Up;
                        break;
                    case "down":
                        change = ClusterChange.Down;
                        break;
                    case "new_node":
                        change = ClusterChange.New;
                        break;
                    case "removed_node":
                        change = ClusterChange.Removed;
                        break;
                    default:
                        return;
                }

                if (OnClusterChange != null)
                    OnClusterChange(this, new ClusterChangedEvent(change, frame.Node.Address));
            }
        }

        /// <summary>
        /// Returns a <see cref="System.String" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format("Connection {0} #{1}", Address, _nr);
        }
    }
}