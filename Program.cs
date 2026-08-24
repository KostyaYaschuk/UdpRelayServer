using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Threading.Channels;

var builder = WebApplication.CreateSlimBuilder(args);

var port = Environment.GetEnvironmentVariable("PORT") ?? "8080";
builder.WebHost.UseUrls($"http://0.0.0.0:{port}");

var app = builder.Build();

app.UseWebSockets(new WebSocketOptions
{
    KeepAliveInterval = TimeSpan.FromSeconds(10)
});

const string SECRET_KEY = "my_super_secret_discord_key";

app.MapGet("/", () => "High-Performance UDP Relay Server is Running!");

app.Map("/relay", async context =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = 400;
        return;
    }

    if (context.Request.Headers["X-Auth-Key"] != SECRET_KEY && 
        context.Request.Query["key"] != SECRET_KEY)
    {
        context.Response.StatusCode = 401;
        return;
    }

    using var webSocket = await context.WebSockets.AcceptWebSocketAsync();
    using var udpClient = new UdpClient(0);

    // 4 MB буферы на Linux сервере
    try
    {
        udpClient.Client.ReceiveBufferSize = 4 * 1024 * 1024;
        udpClient.Client.SendBufferSize = 4 * 1024 * 1024;
    }
    catch { }

    using var cts = new CancellationTokenSource();

    // Неблокирующие очереди (Channels) для мгновенной пересылки
    var toWsChannel = Channel.CreateUnbounded<RentedPacket>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
    var toUdpChannel = Channel.CreateUnbounded<RentedPacket>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });

    // Поток 1: Читаем UDP от Discord -> бросаем в Channel
    var udpReceiveTask = Task.Run(async () =>
    {
        var remoteEp = new IPEndPoint(IPAddress.Any, 0);
        try
        {
            while (!cts.IsCancellationRequested)
            {
                var buffer = ArrayPool<byte>.Shared.Rent(65536);
                var result = await udpClient.Client.ReceiveFromAsync(new ArraySegment<byte>(buffer, 6, 65530), SocketFlags.None, remoteEp);
                var ep = (IPEndPoint)result.RemoteEndPoint;

                var ipBytes = ep.Address.GetAddressBytes();
                ushort remotePort = (ushort)ep.Port;

                buffer[0] = ipBytes[0];
                buffer[1] = ipBytes[1];
                buffer[2] = ipBytes[2];
                buffer[3] = ipBytes[3];
                buffer[4] = (byte)(remotePort >> 8);
                buffer[5] = (byte)(remotePort & 0xFF);

                int totalLen = 6 + result.ReceivedBytes;
                toWsChannel.Writer.TryWrite(new RentedPacket(buffer, totalLen, null));
            }
        }
        catch { }
    });

    // Поток 2: Вычитываем Channel -> непрерывно шлем в WebSocket клиенту
    var wsSendTask = Task.Run(async () =>
    {
        try
        {
            var reader = toWsChannel.Reader;
            while (await reader.WaitToReadAsync(cts.Token))
            {
                while (reader.TryRead(out var packet))
                {
                    try
                    {
                        if (webSocket.State == WebSocketState.Open)
                        {
                            await webSocket.SendAsync(new ReadOnlyMemory<byte>(packet.Buffer, 0, packet.Length), WebSocketMessageType.Binary, true, cts.Token);
                        }
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(packet.Buffer);
                    }
                }
            }
        }
        catch { }
    });

    // Поток 3: Читаем из WebSocket -> бросаем в Channel для UDP
    var wsReceiveTask = Task.Run(async () =>
    {
        try
        {
            while (!cts.IsCancellationRequested && webSocket.State == WebSocketState.Open)
            {
                var buffer = ArrayPool<byte>.Shared.Rent(65536);
                var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), cts.Token);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                    break;
                }

                if (result.Count > 6)
                {
                    var targetIp = new IPAddress(buffer.AsSpan(0, 4));
                    var targetPort = (ushort)((buffer[4] << 8) | buffer[5]);
                    var ep = new IPEndPoint(targetIp, targetPort);

                    toUdpChannel.Writer.TryWrite(new RentedPacket(buffer, result.Count, ep));
                }
                else
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
        }
        catch { }
    });

    // Поток 4: Вычитываем Channel -> шлем UDP в Discord
    var udpSendTask = Task.Run(async () =>
    {
        try
        {
            var reader = toUdpChannel.Reader;
            while (await reader.WaitToReadAsync(cts.Token))
            {
                while (reader.TryRead(out var packet))
                {
                    try
                    {
                        if (packet.EndPoint != null)
                        {
                            var data = new ReadOnlyMemory<byte>(packet.Buffer, 6, packet.Length - 6);
                            await udpClient.SendAsync(data, packet.EndPoint, cts.Token);
                        }
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(packet.Buffer);
                    }
                }
            }
        }
        catch { }
    });

    await Task.WhenAny(wsReceiveTask, udpReceiveTask);
    cts.Cancel();

    toWsChannel.Writer.TryComplete();
    toUdpChannel.Writer.TryComplete();

    await Task.WhenAll(wsSendTask, udpSendTask);

    if (webSocket.State == WebSocketState.Open)
    {
        await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closed", CancellationToken.None);
    }
});

app.Run();

public readonly struct RentedPacket
{
    public readonly byte[] Buffer;
    public readonly int Length;
    public readonly IPEndPoint? EndPoint;

    public RentedPacket(byte[] buffer, int length, IPEndPoint? endPoint)
    {
        Buffer = buffer;
        Length = length;
        EndPoint = endPoint;
    }
}