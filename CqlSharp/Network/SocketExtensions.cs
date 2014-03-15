using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp.Network
{
    internal static class TcpClientExtensions
    {

        /// <summary>
        /// Connects the client to the given remote address and port, using a maximum timeout. The client
        /// will be closed when the timeout has passed and the client is still not connected
        /// </summary>
        /// <param name="client">The client.</param>
        /// <param name="address">The address to connect to.</param>
        /// <param name="port">The port to connect to.</param>
        /// <param name="timeout">The timeout in ms. Set to 0 to skip timeouts</param>
        /// <exception cref="System.IO.IOException">When a timeout has occured. In this case the socket will have been closed.</exception>
        public static async Task ConnectAsync(this TcpClient client, IPAddress address, int port, int timeout)
        {
            //create connection timeout token
            var token = timeout > 0
                            ? new CancellationTokenSource(timeout).Token
                            : CancellationToken.None;
            try
            {
                //close connection when timeout is invoked
                using (token.Register((cl) => ((TcpClient)cl).Close(), client))
                {
                    await client.ConnectAsync(address, port).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                if (token.IsCancellationRequested)
                {
                    var error = string.Format("Connection to {0}:{1} could not be established within set timeout",
                                              address, port);

                    throw new IOException(error, ex);
                }

                throw;
            }
        }

        /// <summary>
        /// Sets the keep-alive interval for the socket.
        /// </summary>
        /// <param name="client"> </param>
        /// <param name="time">Time between two keep alive "pings".</param>
        public static void SetKeepAlive(this TcpClient client, ulong time)
        {
            SetKeepAlive(client, time, 1000);
        }

        /// <summary>
        /// Sets the keep-alive interval for the socket.
        /// </summary>
        /// <param name="client">TcpClient to set KeepAlive value on</param>
        /// <param name="time">Time between two keep alive "pings".</param>
        /// <param name="interval">Time between two keep alive "pings" when first one fails.</param>
        /// <remarks>based on: http://www.codekeep.net/snippets/269152eb-726b-4cd5-a22d-4e7cef27f93f.aspx </remarks>
        public static void SetKeepAlive(this TcpClient client, ulong time, ulong interval)
        {
            try
            {
                const int bytesPerLong = 4; // 32 / 8
                const int bitsPerByte = 8;

                // Array to hold input values.
                var input = new[]
                {
            	    (time == 0 || interval == 0) ? 0UL : 1UL, // on or off
				    time,
				    interval 
			    };

                // Pack input into byte struct.
                var inValue = new byte[3 * bytesPerLong];
                for (int i = 0; i < input.Length; i++)
                {
                    inValue[i * bytesPerLong + 3] = (byte)(input[i] >> ((bytesPerLong - 1) * bitsPerByte) & 0xff);
                    inValue[i * bytesPerLong + 2] = (byte)(input[i] >> ((bytesPerLong - 2) * bitsPerByte) & 0xff);
                    inValue[i * bytesPerLong + 1] = (byte)(input[i] >> ((bytesPerLong - 3) * bitsPerByte) & 0xff);
                    inValue[i * bytesPerLong + 0] = (byte)(input[i] >> ((bytesPerLong - 4) * bitsPerByte) & 0xff);
                }

                // Create bytestruct for result (bytes pending on server socket).
                byte[] outValue = BitConverter.GetBytes(0);

                // Write SIO_VALS to Socket IOControl.
                client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                client.Client.IOControl(IOControlCode.KeepAliveValues, inValue, outValue);
            }
            catch (Exception ex)
            {
                throw new IOException("Could not set Socket Keep Alive values", ex);
            }

        }
    }
}
