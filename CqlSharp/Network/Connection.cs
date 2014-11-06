// CqlSharp - CqlSharp
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
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using CqlSharp.Authentication;
using CqlSharp.Extensions;
using CqlSharp.Logging;
using CqlSharp.Memory;
using CqlSharp.Protocol;
using CqlSharp.Threading;

//suppressing warnings about unobserved tasks
#pragma warning disable 168

namespace CqlSharp.Network
{
    /// <summary>
    /// A single connection to a Cassandra node.
    /// </summary>
    internal class Connection : IDisposable
    {
        private static class ConnectionState
        {
            public const int Created = 0;
            public const int Connecting = 1;
            public const int Connected = 2;
            public const int Closed = 3;
        }

        private readonly Queue<short> _availableQueryIds;
        private short _usedQueryIds;

        private readonly CqlConnectionStringBuilder _config;
        private SemaphoreSlim _frameSubmitLock;
        private readonly long _maxIdleTicks;
        private readonly Node _node;
        private readonly int _nr;
        private readonly IDictionary<short, TaskCompletionSource<Frame>> _openRequests;
        private readonly ManualResetEventSlim _readLoopCompleted;
        private readonly object _syncLock = new object();

        private readonly SemaphoreSlim _writeLock;
        private readonly SemaphoreSlim _readLock;

        private int _activeRequests;
        private bool _allowCompression;
        private TcpClient _client;
        private volatile Task _connectTask;
        private int _connectionState;
        private long _lastActivity;
        private int _load;
        private Stream _readStream;
        private Stream _writeStream;
        
        /// <summary>
        /// Initializes a new instance of the <see cref="Connection" /> class.
        /// </summary>
        /// <param name="node"> The node. </param>
        /// <param name="nr"> The connection nr. </param>
        public Connection(Node node, int nr)
        {
            _node = node;
            _nr = nr;
            _config = node.Cluster.Config;

            //setup support for multiple queries
            _availableQueryIds = new Queue<short>();
            _usedQueryIds = 0;
            _openRequests = new Dictionary<short, TaskCompletionSource<Frame>>();

            //setup locks
            _writeLock = new SemaphoreSlim(1);
            _readLock = new SemaphoreSlim(0, int.MaxValue);
            _readLoopCompleted = new ManualResetEventSlim(true);
            //_frameSubmitLock is initialized later when protocol version is known

            //setup state
            _activeRequests = 0;
            _load = 0;
            _connectionState = ConnectionState.Created;
            _lastActivity = DateTime.UtcNow.Ticks;

            _maxIdleTicks = TimeSpan.FromSeconds(_config.MaxConnectionIdleTime).Ticks;
            AllowCleanup = true;

            Logger.Current.LogVerbose("{0} created", this);
        }

        /// <summary>
        /// Gets the current key space.
        /// </summary>
        /// <value> The current key space. </value>
        internal string CurrentKeySpace { get; private set; }

        /// <summary>
        /// Gets the load.
        /// </summary>
        /// <value> The load. </value>
        public int Load
        {
            get { return _load; }
        }

        /// <summary>
        /// Gets the node.
        /// </summary>
        /// <value> The node. </value>
        public Node Node
        {
            get { return _node; }
        }

        /// <summary>
        /// Gets a value indicating whether this instance is idle. An connection is idle if it
        /// is connected and when the load is zero and the last activity is older
        /// than the configured MaxConnectionIdleTime, and cleanup is allowed.
        /// </summary>
        /// <value> <c>true</c> if this instance is idle; otherwise, <c>false</c> . </value>
        public bool IsIdle
        {
            get
            {
                return _connectionState == ConnectionState.Connected &&
                       AllowCleanup && 
                       _activeRequests == 0 &&
                       (DateTime.UtcNow.Ticks - Interlocked.Read(ref _lastActivity)) > _maxIdleTicks;
            }
        }

        /// <summary>
        /// Gets the number of active requests.
        /// </summary>
        /// <value> The active requests. </value>
        public int ActiveRequests
        {
            get { return _activeRequests; }
        }

        /// <summary>
        /// Gets a value indicating whether this instance is available for executing queries.
        /// </summary>
        /// <value> <c>true</c> if this instance is available; otherwise, <c>false</c> . </value>
        public bool IsAvailable
        {
            get
            {
                var state = _connectionState;
                return state == ConnectionState.Created || 
                       state == ConnectionState.Connecting ||
                       state == ConnectionState.Connected;
            }
        }

        /// <summary>
        /// Gets the address.
        /// </summary>
        /// <value> The address. </value>
        public IPAddress Address
        {
            get { return _node.Address; }
        }

        /// <summary>
        /// Indicates whether automatic cleanup of this connection is allowed. Typically set to prevent
        /// a connection to be cleaned up, when it is exclusively reserved for use by an application
        /// </summary>
        public bool AllowCleanup { get; set; }

        /// <summary>
        /// Gets the protocol version.
        /// </summary>
        /// <value>
        /// The protocol version.
        /// </value>
        public byte ProtocolVersion
        {
            get { return _node.ProtocolVersion; }
        }

        /// <summary>
        /// Occurs when [on connection change].
        /// </summary>
        public event EventHandler<ConnectionChangeEvent> OnConnectionChange;

        /// <summary>
        /// Occurs when [on load change].
        /// </summary>
        public event EventHandler<LoadChangeEvent> OnLoadChange;

        /// <summary>
        /// Occurs when [on cluster change].
        /// </summary>
        public event EventHandler<ClusterChangedEvent> OnClusterChange;

        /// <summary>
        /// Updates the load of this connection, and will trigger a corresponding event
        /// </summary>
        /// <param name="load"> The load. </param>
        /// <param name="logger"> The logger. </param>
        private void UpdateLoad(int load, Logger logger)
        {
            var newLoad = Interlocked.Add(ref _load, load);
            Interlocked.Exchange(ref _lastActivity, DateTime.UtcNow.Ticks);

            EventHandler<LoadChangeEvent> handler = OnLoadChange;
            if (handler != null) handler(this, new LoadChangeEvent { LoadDelta = load });

            logger.LogVerbose("{0} has now a load of {1}", this, newLoad);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            //close the connection
            Close(false);
        }

        /// <summary>
        /// Closes the TCP connection underlying this connection if any
        /// </summary>
        private void Disconnect()
        {
            //close client if it exists
            TcpClient client = Interlocked.Exchange(ref _client, null);
            if (client != null)
            {
                //close client
                client.Close();

                //signal that this connection is closed
                Logger.Current.LogVerbose("TCP connection of {0} closed.", this);

                //signal readLock to allow readloop to cleanup
                _readLock.Release();
            }
        }

        /// <summary>
        /// Closes this connection and moves it into closed state
        /// </summary>
        /// <param name="failed">if set to <c>true</c> [failed].</param>
        private void Close(bool failed)
        {
            int previousState = Interlocked.Exchange(ref _connectionState, ConnectionState.Closed);
            if (previousState != ConnectionState.Closed)
            {
                //Disconnect the connection
                Disconnect();

                //signal that connection closed
                if (OnConnectionChange != null)
                {
                    OnConnectionChange(this, new ConnectionChangeEvent { Failure = failed, Connected = false });
                }

                //update load figures of the parent
                UpdateLoad(-Load, Logger.Current);

                //dispose remaining locks
                if (_frameSubmitLock != null) _frameSubmitLock.Dispose();
                _writeLock.Dispose();
                _readLoopCompleted.Dispose();

                //clear callbacks
                OnConnectionChange = null;
                OnLoadChange = null;
                OnClusterChange = null;

                //log closing of connection
                Logger.Current.LogInfo("{0} closed.", this);

            }
        }

        /// <summary>
        /// Opens the connection. The actual open sequence will be executed at most once.
        /// </summary>
        /// <returns> Task that represents the open procedure for this connection </returns>
        public Task OpenAsync(Logger logger)
        {
            if (_connectionState == ConnectionState.Closed)
                throw new ObjectDisposedException(ToString());

            if (_connectTask == null)
            {
                lock (_syncLock)
                {
                    if (_connectTask == null)
                        _connectTask = OpenAsyncInternal(logger);
                }
            }

            return _connectTask;
        }

        /// <summary>
        /// Opens the connection. Called once per connection only
        /// </summary>
        private async Task OpenAsyncInternal(Logger logger)
        {
            //set state to connecting
            int previousState = Interlocked.CompareExchange(ref _connectionState, ConnectionState.Connecting, ConnectionState.Created);

            if (previousState == ConnectionState.Closed)
                throw new ObjectDisposedException(ToString());

            if (previousState != ConnectionState.Created)
                throw new InvalidOperationException("Opening a connection that is already connected!");

            try
            {
                while(true)
                {
                    //connect
                    await ConnectAsync().AutoConfigureAwait();

                    //get streams
                    Stream tcpStream = _client.GetStream();
                    _writeStream = tcpStream;
                    _readStream = tcpStream;

                    logger.LogVerbose("TCP connection for {0} is opened", this);

                    //start readloop
                    Scheduler.RunOnIOThread((Action)ReadFramesAsync);

                    try
                    {
                        logger.LogVerbose("Attempting to connect using protocol version {0}", Node.ProtocolVersion);

                        await NegotiateConnectionOptionsAsync(logger).AutoConfigureAwait();
                        break;
                    }
                    catch(ProtocolException pex)
                    {
                        //In case of a protocol version mismatch, Cassandra will reply with an error 
                        //using the supported protocol version. If we are using the correct version 
                        //something else is wrong, and it is no use to retry with a different version, 
                        //so rethrow
                        if(Node.ProtocolVersion == pex.ProtocolVersion)
                            throw;

                        logger.LogVerbose(
                            "Failed connecting using protocol version {0}, retrying with protocol version {1}...",
                            Node.ProtocolVersion, pex.ProtocolVersion);

                        //set protocol version to the one received
                        Node.ProtocolVersion = pex.ProtocolVersion;

                        //close the connection (required as protocols are not backwards compatible, so stream may be corrupt now)
                        using(logger.ThreadBinding())
                            Disconnect();

                        //wait until the readloop has stopped
                        _readLoopCompleted.Wait();
                    }
                }

                //run the startup message exchange
                await StartupAsync(logger).AutoConfigureAwait();

                //yeah, connected
                previousState = Interlocked.CompareExchange(ref _connectionState, ConnectionState.Connected, ConnectionState.Connecting);
                if(previousState!=ConnectionState.Connecting)
                    throw new ObjectDisposedException(ToString(), "Connection closed while opening");

                //notify connection changed
                using(logger.ThreadBinding())
                {
                    if(OnConnectionChange != null)
                        OnConnectionChange(this, new ConnectionChangeEvent {Connected = true});
                }

                logger.LogInfo("{0} is opened using Cql Protocol v{1}", this, Node.ProtocolVersion);
            }
            catch (Exception)
            {
                using (logger.ThreadBinding())
                {
                    Close(true);
                    throw;
                }
            }
        }

        /// <summary>
        /// Creates an underlying TCP connection
        /// </summary>
        /// <returns></returns>
        private Task ConnectAsync()
        {
            // ReSharper disable once UseObjectOrCollectionInitializer
            //create TCP connection
            _client = new TcpClient();

            //set buffer sizes
            _client.SendBufferSize = _config.SocketSendBufferSize;
            _client.ReceiveBufferSize = _config.SocketReceiveBufferSize;

            //set keepAlive if requested
            if (_config.SocketKeepAlive > 0) _client.SetKeepAlive((ulong)_config.SocketKeepAlive);

            //set Linger State
            var lingerState = _config.SocketSoLinger >= 0
                ? new LingerOption(true, _config.SocketSoLinger)
                : new LingerOption(false, 0);
            _client.LingerState = lingerState;

            //connect within requested timeout
            return _client.ConnectAsync(_node.Address, _config.Port, _config.SocketConnectTimeout);
        }

        /// <summary>
        /// Negotiates the connection options.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <returns></returns>
        /// <exception cref="ProtocolException">0;Expected Supported frame not received</exception>
        private async Task NegotiateConnectionOptionsAsync(Logger logger)
        {
            //get options from server
            var options = new OptionsFrame();

            var frame =
                await SendRequestAsyncInternal(options, logger, 1, CancellationToken.None).AutoConfigureAwait();
            
            var supported = frame as SupportedFrame;
            if (supported == null)
                throw new ProtocolException(frame.ProtocolVersion, 0, "Expected Supported frame not received");

            //setup concurrent calls depending on frameversion
            _frameSubmitLock = new SemaphoreSlim(supported.ProtocolVersion <= 2 ? sbyte.MaxValue : short.MaxValue);

            //setuo compression
            _allowCompression = false;
            if (_config.AllowCompression)
            {
                IList<string> compressionOptions;
                //check if options contain compression
                if (supported.SupportedOptions.TryGetValue("COMPRESSION", out compressionOptions))
                {
                    //check wether snappy is supported
                    _allowCompression = compressionOptions.Contains("snappy");
                }
            }

            //dispose supported frame
            supported.Dispose();
        }

        /// <summary>
        /// Startups the connection using the required message exchange
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <returns></returns>
        /// <exception cref="ProtocolException">0;Expected Ready frame not received</exception>
        private async Task StartupAsync(Logger logger)
        {
            //submit startup frame
            var startup = new StartupFrame(_config.CqlVersion);
            if (_allowCompression)
            {
                logger.LogVerbose("Enabling Snappy Compression.");
                startup.Options["COMPRESSION"] = "snappy";
            }

            Frame response =
                await
                    SendRequestAsyncInternal(startup, logger, 1, CancellationToken.None).AutoConfigureAwait();

            //authenticate if required
            var auth = response as AuthenticateFrame;
            if (auth != null)
                await AuthenticateAsync(auth, logger).AutoConfigureAwait();

            //no authenticate frame, so ready frame must be received
            else if (!(response is ReadyFrame))
                throw new ProtocolException(response.ProtocolVersion, 0, "Expected Ready frame not received", response.TracingId);

            //dispose ready frame
            response.Dispose();
        }

        /// <summary>
        /// Authenticates the connection.
        /// </summary>
        /// <param name="auth">The authentication request from the server.</param>
        /// <param name="logger">The logger.</param>
        /// <returns></returns>
        /// <exception cref="AuthenticationException">
        /// Unsupported Authenticator:  + auth.Authenticator;null
        /// or
        /// Authentication failed, SASL Challenge was rejected by client
        /// or
        /// Authentication failed, Authenticator rejected SASL result
        /// or
        /// Expected a Authentication Challenge from Server!
        /// or
        /// No credentials provided in configuration
        /// or
        /// Authentication failed: Ready frame not received
        /// </exception>
        private async Task AuthenticateAsync(AuthenticateFrame auth, Logger logger)
        {
            logger.LogVerbose("Authentication requested, attempting to provide credentials");

            //dispose AuthenticateFrame
            auth.Dispose();

            if (auth.ProtocolVersion >= 2)
            {
                //protocol version2: use SASL AuthResponse to authenticate

                //get an AuthenticatorFactory
                IAuthenticatorFactory factory =
                    Loader.Extensions.AuthenticationFactories.FirstOrDefault(
                        f => f.Name.Equals(auth.Authenticator, StringComparison.OrdinalIgnoreCase));

                if (factory == null)
                    throw new AuthenticationException(auth.ProtocolVersion, "Unsupported Authenticator: " + auth.Authenticator);

                logger.LogVerbose("Attempting authentication for scheme {0}", factory.Name);

                //grab an authenticator instance
                IAuthenticator authenticator = factory.CreateAuthenticator(_config);

                //start authentication loop
                byte[] saslChallenge = null;
                while (true)
                {
                    //check for challenge
                    byte[] saslResponse;
                    if (!authenticator.Authenticate(auth.ProtocolVersion, saslChallenge, out saslResponse))
                    {
                        throw new AuthenticationException(auth.ProtocolVersion, "Authentication failed, SASL Challenge was rejected by client");
                    }

                    //send response
                    var cred = new AuthResponseFrame(saslResponse);
                    var authResponse =
                        await
                            SendRequestAsyncInternal(cred, logger, 1, CancellationToken.None).AutoConfigureAwait();

                    //dispose authResponse (makes sure all is read)
                    authResponse.Dispose();

                    //check for success
                    var success = authResponse as AuthSuccessFrame;
                    if (success != null)
                    {
                        if (!authenticator.Authenticate(auth.ProtocolVersion, success.SaslResult))
                        {
                            throw new AuthenticationException(authResponse.ProtocolVersion, "Authentication failed, Authenticator rejected SASL result", authResponse.TracingId);
                        }

                        //yeah, authenticated, break from the authentication loop
                        break;
                    }

                    //no success yet, lets try next round
                    var challenge = authResponse as AuthChallengeFrame;
                    if (challenge == null)
                    {
                        throw new AuthenticationException(authResponse.ProtocolVersion, "Expected a Authentication Challenge from Server!", authResponse.TracingId);
                    }

                    saslChallenge = challenge.SaslChallenge;
                }
            }
            else
            {
                //protocol version1: use Credentials to authenticate

                //check if _username is actually set
                if (_config.Username == null || _config.Password == null)
                    throw new AuthenticationException(auth.ProtocolVersion, "No credentials provided in configuration");

                var cred = new CredentialsFrame(_config.Username, _config.Password);
                var authResponse =
                    await
                        SendRequestAsyncInternal(cred, logger, 1, CancellationToken.None).AutoConfigureAwait();

                //dispose authResponse (makes sure all is read)
                authResponse.Dispose();

                if (!(authResponse is ReadyFrame))
                {
                    throw new AuthenticationException(authResponse.ProtocolVersion, "Authentication failed: Ready frame not received", authResponse.TracingId);
                }
            }
        }

        /// <summary>
        /// Submits a frame, and waits until response is received
        /// </summary>
        /// <param name="frame"> The frame to send. </param>
        /// <param name="logger"> logger to write progress to </param>
        /// <param name="load"> the load indication of the request. Used for balancing queries over nodes and connections </param>
        /// <param name="token"> The token. </param>
        /// <returns> </returns>
        public Task<Frame> SendRequestAsync(Frame frame, Logger logger, int load, CancellationToken token)
        {
            if (_connectionState == ConnectionState.Closed)
                throw new ObjectDisposedException(ToString());

            if(_connectionState == ConnectionState.Connected && !token.CanBeCanceled)
                return SendRequestAsyncInternal(frame, logger, load, token);

            return SendRequestAsyncComplex(frame, logger, load, token);
        }

        /// <summary>
        ///  Submits a frame, and waits until response is received (complex version)
        /// </summary>
        /// <param name="frame"> The frame to send. </param>
        /// <param name="logger"> logger to write progress to </param>
        /// <param name="load"> the load indication of the request. Used for balancing queries over nodes and connections </param>
        /// <param name="token"> The token. </param>
        /// <returns></returns>
        private async Task<Frame> SendRequestAsyncComplex(Frame frame, Logger logger, int load, CancellationToken token)
        {
            //make sure connection is open
            await OpenAsync(logger).AutoConfigureAwait();

            //send request
            var requestTask = SendRequestAsyncInternal(frame, logger, load, token);
            
            //take fast path if we can skip cancellation support
            if(!token.CanBeCanceled)
                return await requestTask.AutoConfigureAwait();
            
            //setup task that completes when token is set to cancelled
            var cancelTask = new TaskCompletionSource<bool>();
            using (token.Register(s => ((TaskCompletionSource<bool>)s).TrySetResult(true), cancelTask))
            {
                //wait for either sendTask or cancellation task to complete
                if (requestTask != await Task.WhenAny(requestTask, cancelTask.Task).AutoConfigureAwait())
                {
                    //ignore/log any exception of the handled task
                    // ReSharper disable once UnusedVariable
                    var logError = requestTask.ContinueWith((sendTask, log) =>
                    {
                        if (sendTask.Exception == null)
                            return;

                        var logger1 = (Logger)log;
                        logger1.LogWarning(
                            "Cancelled query threw exception: {0}",
                            sendTask.Exception.InnerException);
                    }, logger,
                                                     TaskContinuationOptions.OnlyOnFaulted |
                                                     TaskContinuationOptions.ExecuteSynchronously);

                    //get this request cancelled
                    throw new OperationCanceledException(token);
                }
            }
            return await requestTask.AutoConfigureAwait();
        }

        /// <summary>
        /// Sends the request async internal. Cancellation supported until request is send, after which answer must be handled
        /// to avoid connection corruption.
        /// </summary>
        /// <param name="frame"> The frame. </param>
        /// <param name="logger"> The logger. </param>
        /// <param name="load"> The load. </param>
        /// <param name="token"> The token. </param>
        /// <returns> </returns>
        /// <exception cref="System.IO.IOException">Not connected</exception>
        private async Task<Frame> SendRequestAsyncInternal(Frame frame, Logger logger, int load, CancellationToken token)
        {
            try
            {
                //make sure we aren't disposed
                if(_connectionState == ConnectionState.Closed)
                    throw new ObjectDisposedException(ToString());

                //count the operation
                Interlocked.Increment(ref _activeRequests);

                if (_connectionState == ConnectionState.Connected)
                {
                    //increase the load
                    UpdateLoad(load, logger);
                    
                    //wait until frame id is available to submit a frame
                    logger.LogVerbose("Waiting for connection lock on {0}...", this);
                    if (Scheduler.RunningSynchronously)
                        _frameSubmitLock.Wait(token);
                    else
                        await _frameSubmitLock.WaitAsync(token).AutoConfigureAwait();
                }

                //get a task that gets completed when a response is received
                var waitTask = new TaskCompletionSource<Frame>();

                //get a stream id, and store wait task under that id
                short id;
                lock (_availableQueryIds)
                {
                    id = _availableQueryIds.Count > 0 ? _availableQueryIds.Dequeue() : _usedQueryIds++;
                    _openRequests.Add(id, waitTask);
                }

                try
                {
                    //send frame
                    frame.Stream = id;

                    //set protocol version in use
                    frame.ProtocolVersion = Node.ProtocolVersion;

                    //serialize frame outside lock
                    PoolMemoryStream frameBytes = frame.GetFrameBytes(_allowCompression && (_connectionState!=ConnectionState.Connecting),
                                                                      _config.CompressionTreshold);

                    //wait to get access to stream
                    if (Scheduler.RunningSynchronously)
                        _writeLock.Wait(token);
                    else
                        await _writeLock.WaitAsync(token).AutoConfigureAwait();

                    try
                    {
                        //make very sure we aren't disposed
                        if (_connectionState == ConnectionState.Closed)
                            throw new ObjectDisposedException(ToString());

                        logger.LogVerbose("Sending {0} Frame with Id {1} over {2}", frame.OpCode, id, this);

                        //write frame to stream, don't use cancelToken to prevent half-written frames
                        if (Scheduler.RunningSynchronously)
                            frameBytes.CopyTo(_writeStream);
                        else
                            await frameBytes.CopyToAsync(_writeStream).AutoConfigureAwait();

                        //unblock readloop to read result
                        _readLock.Release();
                    }
                    finally
                    {
                        _writeLock.Release();
                        frameBytes.Dispose();
                    }

                    //wait until response is received
                    Frame response = await waitTask.Task.AutoConfigureAwait();

                    logger.LogVerbose("Received {0} Frame with Id {1} on {2}", response.OpCode, id,
                                      this);

                    //read frame content
                    await response.ReadFrameContentAsync().AutoConfigureAwait();

                    //throw error if result is an error
                    var error = response as ErrorFrame;
                    if (error != null)
                    {
                        //dispose error frame
                        error.Dispose();

                        //throw exception
                        throw error.Exception;
                    }

                    //check for keyspace change
                    var keyspaceChange = response as ResultFrame;
                    if (keyspaceChange != null && keyspaceChange.CqlResultType == CqlResultType.SetKeyspace)
                    {
                        logger.LogVerbose("{0} changed KeySpace to \"{1}\"", this, keyspaceChange.Keyspace);
                        CurrentKeySpace = keyspaceChange.Keyspace;
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

                    if (_connectionState == ConnectionState.Connected)
                    {
                        //allow another frame to be send
                        _frameSubmitLock.Release();

                        //reduce load, we are done
                        UpdateLoad(-load, logger);
                    }

                    //decrease the amount of operations
                    Interlocked.Decrement(ref _activeRequests);
                }
            }
            catch (OperationCanceledException)
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
                            Close(true);
                            throw;
                        }

                    default:
                        //some other Cql error (syntax ok?), simply rethrow
                        throw;
                }
            }
            catch (ObjectDisposedException odex)
            {
                throw new IOException("Connection closed while processing request", odex);
            }
            catch (Exception)
            {
                using (logger.ThreadBinding())
                {
                    //connection collapsed, dispose this connection
                    Close(true);
                    throw;
                }
            }
        }

        /// <summary>
        /// Starts a readloop
        /// </summary>
        private async void ReadFramesAsync()
        {
            var logger = _node.Cluster.LoggerManager.GetLogger("CqlSharp.Connection.ReadLoop");

            //signal that we are not done reading
            _readLoopCompleted.Reset();

            //while connected do
            while (_connectionState != ConnectionState.Closed)
            {
                try
                {
                    //logger.LogVerbose("Waiting for new frame to arrive on {0}", this);

                    //wait until signal that reading can commence
                    await _readLock.WaitAsync().AutoConfigureAwait();

                    //check if we are still connected, if not exit
                    if(_client==null)
                    {
                        logger.LogVerbose("Exiting readloop of {0} as TCP connection is disposed", this);
                        break;
                    }

                    //read next frame from stream
                    Frame frame = await Frame.FromStream(_readStream).AutoConfigureAwait();

                    //frame may be null, when the stream was closed gracefully by Cassandra
                    if(frame == null)
                    {
                        using(logger.ThreadBinding())
                        {
                            Close(false);
                            break;
                        }
                    }

                    //check if frame is event
                    if (frame.Stream == -1)
                    {
                        //load frame content
                        await frame.ReadFrameContentAsync().AutoConfigureAwait();

                        var eventFrame = frame as EventFrame;
                        if (eventFrame == null)
                        {
                            throw new ProtocolException(frame.ProtocolVersion, ErrorCode.Protocol, "A frame is received with StreamId -1, while it is not an EventFrame", frame.TracingId);
                        }

                        logger.LogVerbose("Event frame received on {0}", this);

                        //run the event logic in its own task, making sure it does not delay further reading
                        Scheduler.RunOnThreadPool(() => ProcessEvent(eventFrame));
                        continue;
                    }

                    //get the request waiting on this response
                    TaskCompletionSource<Frame> openRequest;
                    lock (_availableQueryIds)
                    {
                        if (!_openRequests.TryGetValue(frame.Stream, out openRequest))
                            throw new ProtocolException(frame.ProtocolVersion, ErrorCode.Protocol, "Frame with unknown Stream received");
                    }

                    //signal frame received. As a new task, because task
                    //completions may be continued synchronously, potentially
                    //leading to deadlocks when the continuation sends another request
                    //on this connection.
                    //Scheduler.RunOnIOThread(() => openRequest.TrySetResult(frame));
                    openRequest.TrySetResult(frame);

                    //wait until all frame data is read (especially important for queries and results)
                    //logger.LogVerbose("Waiting for frame content to be read from {0}", this);
                    await frame.WaitOnBodyRead().AutoConfigureAwait();
                }
                catch (Exception)
                {
                    using (logger.ThreadBinding())
                    {
                        //error occured during read operaton, assume connection is dead, switch state
                        Close(true);
                    }
                }
            }

            //we stopped reading, fail all other open requests
            List<TaskCompletionSource<Frame>> unfinishedRequests;
            lock (_availableQueryIds)
            {
                unfinishedRequests = new List<TaskCompletionSource<Frame>>(_openRequests.Values.Where(tcs => !(tcs.Task.IsCanceled || tcs.Task.IsFaulted || tcs.Task.IsCompleted)));
                if (unfinishedRequests.Count > 0)
                {
                    logger.LogWarning("{0} closed, throwing connection closed error for {1} queries", this,
                                      unfinishedRequests.Count);
                }
            }

            //iterate over all open request and finish them with an exception
            var closedException = new IOException("Connection closed before receiving a result.");
            foreach (var req in unfinishedRequests)
                req.TrySetException(closedException);

            _readLoopCompleted.Set();
        }

        /// <summary>
        /// Registers for cluster changes.
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="System.InvalidOperationException">Must be connected before Registration can take place</exception>
        /// <exception cref="CqlException">Could not register for cluster changes!</exception>
        public async Task RegisterForClusterChangesAsync(Logger logger)
        {
            if (_connectionState == ConnectionState.Closed)
                throw new ObjectDisposedException(ToString());

            //make sure the connection is open
            await OpenAsync(logger);

            //send the register frame
            var registerframe = new RegisterFrame(new List<string> { "TOPOLOGY_CHANGE", "STATUS_CHANGE" });
            Frame result =
                await SendRequestAsyncInternal(registerframe, logger, 1, CancellationToken.None).AutoConfigureAwait();

            //check result
            if (!(result is ReadyFrame))
                throw new CqlException("Could not register for cluster changes!");

            //increase request count to prevent connection to go in Idle state
            Interlocked.Increment(ref _activeRequests);

            //release readLock to have readloop read the next event
            _readLock.Release();
        }

        /// <summary>
        /// Processes the event frame.
        /// </summary>
        /// <param name="frame"> The frame. </param>
        private void ProcessEvent(EventFrame frame)
        {
            //allow next event to arrive on readloop
            _readLock.Release();

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
        /// <returns> A <see cref="System.String" /> that represents this instance. </returns>
        public override string ToString()
        {
            return string.Format("Connection {0} #{1}", Address, _nr);
        }
    }
}