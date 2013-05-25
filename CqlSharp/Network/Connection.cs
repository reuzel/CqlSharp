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

using CqlSharp.Config;
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
        private readonly ClusterConfig _config;
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


        /// <summary>
        ///   Initializes a new instance of the <see cref="Connection" /> class.
        /// </summary>
        /// <param name="address"> The address. </param>
        /// <param name="config"> The config. </param>
        public Connection(IPAddress address, ClusterConfig config)
        {
            _address = address;
            _config = config;

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
                        (DateTime.Now.Ticks - Interlocked.Read(ref _lastActivity)) > _config.MaxConnectionIdleTime.Ticks);
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
            get { return _connectionState == 1; }
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
        ///   Updates the load of this connection, and will trigger a corresponding event
        /// </summary>
        /// <param name="load"> The load. </param>
        private void UpdateLoad(int load)
        {
            Interlocked.Add(ref _load, load);
            Interlocked.Exchange(ref _lastActivity, DateTime.Now.Ticks);

            EventHandler<LoadChangeEvent> handler = OnLoadChange;
            if (handler != null) handler(this, new LoadChangeEvent { LoadDelta = load });
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
                        if (error != null) _client.LingerState = new LingerOption(true, 0); //TCP reset

                        _client.Close();
                        _client = null;
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
        /// Opens the connection
        /// </summary>
        /// <returns>Task that represents the open procedure for this connection</returns>
        public Task OpenAsync()
        {
            if (_connectTask == null)
            {
                lock (_syncLock)
                {
                    if (_connectTask == null)
                    {
                        _connectTask = OpenAsyncInternal();
                    }
                }
            }

            return _connectTask;
        }

        /// <summary>
        ///   Opens the connection
        /// </summary>
        public async Task OpenAsyncInternal()
        {
            //switch state to connecting if not done so
            int state = Interlocked.CompareExchange(ref _connectionState, 1, 0);

            if (state == 1)
                return;

            if (state == 2)
                throw new ObjectDisposedException("Connction disposed before opening!");

            try
            {
                //create TCP connection
                _client = new TcpClient();
                await _client.ConnectAsync(_address, _config.Port);
                _writeStream = _client.GetStream();
                _readStream = _client.GetStream();

                //start readloop
                StartReading();

                //submit startup frame
                var startup = new StartupFrame(_config.CqlVersion);
                Frame response = await SendRequestAsync(startup, 1, true);

                //authenticate if required
                var auth = response as AuthenticateFrame;
                if (auth != null)
                {
                    //check if _username is actually set
                    if (_config.Username == null || _config.Password == null)
                        throw new UnauthorizedException("No credentials provided");

                    var cred = new CredentialsFrame(_config.Username, _config.Password);
                    response = await SendRequestAsync(cred, 1, true);
                }

                //check if ready
                if (!(response is ReadyFrame))
                    throw new ProtocolException(0, "Expected Ready frame not received");

                if (OnConnectionChange != null)
                    OnConnectionChange(this, new ConnectionChangeEvent { Connected = true });

            }
            catch (Exception ex)
            {
                Dispose(true, ex);
                throw;
            }
        }

        /// <summary>
        ///   Submits a frame, and waits until response is received
        /// </summary>
        /// <param name="frame"> The frame to send. </param>
        /// <param name="load"> the load indication of the request. Used for balancing queries over nodes and connections </param>
        /// <param name="isConnecting">indicates if this request is send as part of connection setup protocol</param>
        /// <returns> </returns>
        internal async Task<Frame> SendRequestAsync(Frame frame, int load = 1, bool isConnecting = false)
        {
            //make sure we are connected
            if (!IsConnected)
                throw new IOException("Not connected");

            try
            {
                //count the operation
                Interlocked.Increment(ref _activeRequests);

                //increase the load
                UpdateLoad(load);

                //wait until allowed to submit a frame
                await _frameSubmitLock.WaitAsync();

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
                    //make sure we're already connected
                    if (!isConnecting)
                        await OpenAsync();

                    //send frame
                    frame.Stream = id;
                    await _writeLock.WaitAsync();
                    try
                    {
                        //final check to make sure we're connected
                        if (_connectionState != 1)
                            throw new IOException("Not connected");

                        await frame.WriteToStream(_writeStream);
                    }
                    finally
                    {
                        _writeLock.Release();
                    }

                    //wait until response is received
                    Frame response = await waitTask.Task;

                    //throw error if result is an error
                    var error = response as ErrorFrame;
                    if (error != null)
                    {
                        throw error.Exception;
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
                    UpdateLoad(-load);
                }
            }
            catch (ProtocolException pex)
            {
                switch (pex.Code)
                {
                    case ErrorCode.Server:
                    case ErrorCode.IsBootstrapping:
                    case ErrorCode.Overloaded:
                    case ErrorCode.Truncate:
                        //IO or node status related error, dispose this connection
                        Dispose(true, pex);
                        throw;

                    default:
                        //some other Cql error (syntax ok?), simply rethrow
                        throw;
                }
            }
            catch (Exception ex)
            {
                //connection collapsed, dispose this connection
                Dispose(true, ex);
                throw;
            }
        }

        /// <summary>
        ///   Starts a readloop
        /// </summary>
        private async void StartReading()
        {
            //while connected do
            while (_connectionState == 1)
            {
                try
                {
                    //read next frame from stream
                    Frame frame = await Frame.FromStream(_readStream);

                    //check if frame is event
                    if (frame.Stream == -1)
                    {
                        var eventFrame = frame as EventFrame;
                        if (eventFrame == null)
                            throw new ProtocolException(ErrorCode.Protocol, "A frame is received with StreamId -1, while it is not an EventFrame");

                        //run the event logic in its own task, making sure it does not delay further reading
                        Task eventTask = Task.Run(() => ProcessEvent(eventFrame));
                        continue;
                    }

                    //get the request waiting on this response
                    TaskCompletionSource<Frame> openRequest;
                    lock (_availableQueryIds)
                    {
                        openRequest = _openRequests[frame.Stream];
                    }

                    //signal frame received. As a new task, because task
                    //completions may be continued synchronously, potentially
                    //leading to deadlocks when the continuation sends another request
                    //on this connection.
                    Task continueOpenRequestTask = Task.Run(() => openRequest.TrySetResult(frame));

                    //wait until all frame data is read (especially important for queries and results)
                    await frame.WaitOnBodyRead();
                }
                catch (Exception ex)
                {
                    //error occured during read operaton, assume connection is dead, switch state
                    Dispose(true, ex);
                }
            }

            //we stopped reading, fail all other open requests
            List<TaskCompletionSource<Frame>> unfinishedRequests;
            lock (_availableQueryIds)
            {
                unfinishedRequests = new List<TaskCompletionSource<Frame>>(_openRequests.Values);
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
        public async Task RegisterForClusterChanges()
        {
            if (!IsConnected)
                throw new InvalidOperationException("Must be connected before Registration can take place");

            var registerframe = new RegisterFrame(new List<string> { "TOPOLOGY_CHANGE", "STATUS_CHANGE" });
            Frame result = await SendRequestAsync(registerframe);

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
    }
}