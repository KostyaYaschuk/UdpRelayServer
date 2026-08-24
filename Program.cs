using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;

// CreateSlimBuilder не создает лишних inotify наблюдателей
var builder = WebApplication.CreateSlimBuilder(args);

var port = Environment.GetEnvironmentVariable("PORT") ?? "8080";
builder.WebHost.UseUrls($"http://0.0.0.0:{port}");

var app = builder.Build();

app.UseWebSockets(new WebSocketOptions
{
    KeepAliveInterval = TimeSpan.FromSeconds(20)
});

const string SECRET_KEY = "my_super_secret_discord_key";

app.MapGet("/", () => "UDP Relay Server is Running!");

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
    var buffer = new byte[65535];
    using var cts = new CancellationTokenSource();

    var udpReceiveTask = Task.Run(async () =>
    {
        try
        {
            while (!cts.IsCancellationRequested && webSocket.State == WebSocketState.Open)
            {
                var result = await udpClient.ReceiveAsync(cts.Token);
                var ipBytes = result.RemoteEndPoint.Address.GetAddressBytes();
                ushort remotePort = (ushort)result.RemoteEndPoint.Port;

                var responseFrame = new byte[6 + result.Buffer.Length];
                responseFrame[0] = ipBytes[0];
                responseFrame[1] = ipBytes[1];
                responseFrame[2] = ipBytes[2];
                responseFrame[3] = ipBytes[3];
                responseFrame[4] = (byte)(remotePort >> 8);
                responseFrame[5] = (byte)(remotePort & 0xFF);
                Buffer.BlockCopy(result.Buffer, 0, responseFrame, 6, result.Buffer.Length);

                await webSocket.SendAsync(responseFrame, WebSocketMessageType.Binary, true, cts.Token);
            }
        }
        catch { }
    });

    try
    {
        while (webSocket.State == WebSocketState.Open)
        {
            var receiveResult = await webSocket.ReceiveAsync(buffer, cts.Token);
            if (receiveResult.MessageType == WebSocketMessageType.Close) break;

            if (receiveResult.Count > 6)
            {
                var targetIp = new IPAddress(buffer.AsSpan(0, 4));
                var targetPort = (ushort)((buffer[4] << 8) | buffer[5]);
                var payload = buffer.AsMemory(6, receiveResult.Count - 6);

                await udpClient.SendAsync(payload, new IPEndPoint(targetIp, targetPort), cts.Token);
            }
        }
    }
    catch { }
    finally
    {
        cts.Cancel();
        await udpReceiveTask;
        if (webSocket.State == WebSocketState.Open)
        {
            await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closed", CancellationToken.None);
        }
    }
});

app.Run();  