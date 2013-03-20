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
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using CqlSharp.Config;
using CqlSharp.Protocol;
using CqlSharp.Protocol.Exceptions;
using CqlSharp.Protocol.Frames;

namespace CqlSharp.Network
{
    /// <summary>
    ///   A single connection to a Cassandra node.
    /// </summary>
    internal class Connection
    {
        private const int MaxStreams = 127;

        private readonly IPAddress _address;

        private readonly Queue<int> _availableQueryIds;
        private readonly ClusterConfig _config;
        private readonly SemaphoreSlim _frameSubmitLock;
        private readonly IDictionary<int, TaskCompletionSource<Frame>> _openRequests;

        private readonly SemaphoreSlim _writeLock;
        private int _activeRequests;
        private TcpClient _client;
        private int _connectionState; //0=disconnected, 1=connected, 2=failed/closed
        private long _lastActivity;
        private int _load;
        private Stream _readStream;
        private Stream _writeStream;


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
            _availableQueryIds = new Queue<int>(Enumerable.Range(1, MaxStreams + 1));
            _openRequests = new Dictionary<int, TaskCompletionSource<Frame>>();

            //setup locks
            _writeLock = new SemaphoreSlim(1);
            _frameSubmitLock = new SemaphoreSlim(MaxStreams);

            //setup state
            _activeRequests = 0;
            _load = 0;
            _connectionState = 0;
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
        ///   Updates the load of this connection, and will trigger a corresponding event
        /// </summary>
        /// <param name="load"> The load. </param>
        private void UpdateLoad(int load)
        {
            Interlocked.Add(ref _load, load);
            Interlocked.Exchange(ref _lastActivity, DateTime.Now.Ticks);

            EventHandler<LoadChangeEvent> handler = OnLoadChange;
            if (handler != null) handler(this, new LoadChangeEvent {LoadDelta = load});
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
                }

                if (OnConnectionChange != null)
                    OnConnectionChange(this, new ConnectionChangeEvent {Exception = error, Connected = false});

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
        ///   Connects to the provided endpoint
        /// </summary>
        /// <exception cref="ProtocolException">0;Expected Ready frame not received</exception>
        public async Task ConnectAsync()
        {
            //switch state to connected if not done so
            int state = Interlocked.CompareExchange(ref _connectionState, 1, 0);

            if (state == 1)
                return; //already connected

            if (state == 2)
                throw new IOException("Connection is closed."); //disconnected

            //go connect
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
                Frame response = await SendRequestAsync(startup);

                //authenticate if required
                var auth = response as AuthenticateFrame;
                if (auth != null)
                {
                    //check if _username is actually set
                    if (_config.Username == null || _config.Password == null)
                        throw new UnauthorizedException("No credentials provided");

                    var cred = new CredentialsFrame(_config.Username, _config.Password);
                    response = await SendRequestAsync(cred);
                }

                //check if ready
                if (!(response is ReadyFrame))
                    throw new ProtocolException(0, "Expected Ready frame not received");

                if (OnConnectionChange != null)
                    OnConnectionChange(this, new ConnectionChangeEvent {Connected = true});
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
        /// <returns> </returns>
        internal async Task<Frame> SendRequestAsync(Frame frame, int load = 1)
        {
            //make sure we are connected
            if (_connectionState != 1)
                throw new IOException("Not connected");

            try
            {
                //count the operation
                Interlocked.Increment(ref _activeRequests);

                //wait until allowed to submit a frame
                await _frameSubmitLock.WaitAsync();

                //get a task that gets completed when a response is received
                var waitTask = new TaskCompletionSource<Frame>();

                //get a stream id, and store wait task under that id
                byte id;
                lock (_availableQueryIds)
                {
                    //final check to make sure we are connected (we may just be killing the connection)
                    if (_connectionState != 1)
                        throw new IOException("Not connected");

                    id = (byte) _availableQueryIds.Dequeue();
                    _openRequests.Add(id, waitTask);
                }

                //increase the load
                UpdateLoad(load);

                try
                {
                    //send frame
                    frame.Stream = id;
                    await _writeLock.WaitAsync();
                    await frame.WriteToStream(_writeStream);
                    _writeLock.Release();

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
            //while packets waiting to be received, do
            while (_connectionState == 1)
            {
                try
                {
                    //read next frame from stream
                    Frame frame = await Frame.FromStream(_readStream);

                    //signal frame received. As a new task, because task
                    //completions may be continued synchronously, potentially
                    //leading to deadlocks when the continuation sends another request
                    //on this connection.
                    TaskCompletionSource<Frame> openRequest = _openRequests[frame.Stream];
                    Task continueOpenRequestTask =
                        Task.Run(() => openRequest.TrySetResult(frame));

                    //wait until all frame data is read (especially important for queries and results)
                    await frame.WaitOnBodyRead();

                    //return stream id to the pool
                    byte id = frame.Stream;
                    lock (_availableQueryIds)
                    {
                        _availableQueryIds.Enqueue(id);
                        _openRequests.Remove(id);
                    }
                }
                catch (Exception ex)
                {
                    //error occured during read operaton, assume connection is dead, switch state
                    Dispose(true, ex);
                }
            }

            lock (_availableQueryIds)
            {
                var ex = new IOException("Connection closed.");

                //iterate over all requests and have them throw an exception
                foreach (var req in _openRequests)
                {
                    //remove request
                    _openRequests.Remove(req.Key);
                    _availableQueryIds.Enqueue(req.Key);

                    //finalize response wait task with exception
                    TaskCompletionSource<Frame> waitTask = req.Value;
                    Task failOpenRequestTask =
                        Task.Run(() => waitTask.TrySetException(ex));
                }
            }
        }
    }
}