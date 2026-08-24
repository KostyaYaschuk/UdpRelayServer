using System.Buffers;
using System.Diagnostics;
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

app.MapGet("/", () => "High-Performance Adaptive UDP Relay Server is Running!");

// Эндпоинт мониторинга для панели управления
app.MapGet("/stats", () =>
{
    var uptime = DateTime.UtcNow - ServerMetrics.StartTime;
    return Results.Json(new
    {
        cpu = ServerMetrics.CpuUsagePercent,
        ramMb = ServerMetrics.MemoryMb,
        uptime = $"{(int)uptime.TotalHours:D2}:{uptime.Minutes:D2}:{uptime.Seconds:D2}",
        packetsIn = ServerMetrics.PacketsInPerSec,
        packetsOut = ServerMetrics.PacketsOutPerSec,
        activeRelays = ServerMetrics.ActiveRelays,
        totalWorkers = ServerMetrics.TotalActiveWorkers,
        cores = Environment.ProcessorCount
    });
});

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

    Interlocked.Increment(ref ServerMetrics.ActiveRelays);

    try
    {
        udpClient.Client.ReceiveBufferSize = 4 * 1024 * 1024;
        udpClient.Client.SendBufferSize = 4 * 1024 * 1024;
    }
    catch { }

    using var cts = new CancellationTokenSource();

    // Очереди (Channels). toUdpChannel поддерживает параллельное чтение несколькими воркерами
    var toWsChannel = Channel.CreateUnbounded<RentedPacket>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
    var toUdpChannel = Channel.CreateUnbounded<RentedPacket>(new UnboundedChannelOptions { SingleReader = false, SingleWriter = true });

    // Параметры адаптивного пула воркеров (от 2 до 100)
    const int MIN_WORKERS = 2;
    const int MAX_WORKERS = 100;
    int targetUdpWorkers = MIN_WORKERS;

    // 1. Поток чтения UDP от Discord -> пишем в WebSocket Channel
    var udpReceiveTask = Task.Run(async () =>
    {
        var remoteEp = new IPEndPoint(IPAddress.Any, 0);
        while (!cts.IsCancellationRequested)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(65536);
            try
            {
                var result = await udpClient.Client.ReceiveFromAsync(new ArraySegment<byte>(buffer, 6, 65530), SocketFlags.None, remoteEp);
                var ep = (IPEndPoint)result.RemoteEndPoint;

                var ipBytes = ep.Address.MapToIPv4().GetAddressBytes();
                ushort remotePort = (ushort)ep.Port;

                buffer[0] = ipBytes[0];
                buffer[1] = ipBytes[1];
                buffer[2] = ipBytes[2];
                buffer[3] = ipBytes[3];
                buffer[4] = (byte)(remotePort >> 8);
                buffer[5] = (byte)(remotePort & 0xFF);

                int totalLen = 6 + result.ReceivedBytes;
                Interlocked.Increment(ref ServerMetrics.TotalPacketsIn);

                if (!toWsChannel.Writer.TryWrite(new RentedPacket(buffer, totalLen, null)))
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
            catch (OperationCanceledException) { ArrayPool<byte>.Shared.Return(buffer); break; }
            catch { ArrayPool<byte>.Shared.Return(buffer); }
        }
    });

    // 2. Поток отправки в WebSocket клиенту
    var wsSendTask = Task.Run(async () =>
    {
        try
        {
            var reader = toWsChannel.Reader;
            while (await reader.WaitToReadAsync(CancellationToken.None))
            {
                while (reader.TryRead(out var packet))
                {
                    try
                    {
                        if (webSocket.State == WebSocketState.Open && !cts.IsCancellationRequested)
                        {
                            await webSocket.SendAsync(new ReadOnlyMemory<byte>(packet.Buffer, 0, packet.Length), WebSocketMessageType.Binary, true, cts.Token);
                        }
                    }
                    catch { }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(packet.Buffer);
                    }
                }
            }
        }
        catch { }
    });

    // 3. Поток чтения из WebSocket от клиента -> бросаем в очередь toUdpChannel
    var wsReceiveTask = Task.Run(async () =>
    {
        while (!cts.IsCancellationRequested && webSocket.State == WebSocketState.Open)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(65536);
            try
            {
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

                    Interlocked.Increment(ref ServerMetrics.TotalPacketsOut);

                    if (!toUdpChannel.Writer.TryWrite(new RentedPacket(buffer, result.Count, ep)))
                    {
                        ArrayPool<byte>.Shared.Return(buffer);
                    }
                }
                else
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
            catch { ArrayPool<byte>.Shared.Return(buffer); break; }
        }
    });

    // 4. Метод единичного параллельного UDP воркера
    async Task RunUdpWorkerAsync(int workerId)
    {
        Interlocked.Increment(ref ServerMetrics.TotalActiveWorkers);
        try
        {
            var reader = toUdpChannel.Reader;
            while (!cts.IsCancellationRequested)
            {
                // Проверка: если лимит воркеров уменьшился из-за высокой нагрузки CPU — завершаем лишний воркер
                if (workerId > Volatile.Read(ref targetUdpWorkers))
                {
                    break;
                }

                if (!await reader.WaitToReadAsync(CancellationToken.None))
                    break;

                while (reader.TryRead(out var packet))
                {
                    try
                    {
                        if (packet.EndPoint != null && !cts.IsCancellationRequested)
                        {
                            var data = new ReadOnlyMemory<byte>(packet.Buffer, 6, packet.Length - 6);
                            await udpClient.SendAsync(data, packet.EndPoint, cts.Token);
                        }
                    }
                    catch { }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(packet.Buffer);
                    }

                    // Проверка на плавное масштабирование вниз
                    if (workerId > Volatile.Read(ref targetUdpWorkers))
                    {
                        return;
                    }
                }
            }
        }
        finally
        {
            Interlocked.Decrement(ref ServerMetrics.TotalActiveWorkers);
        }
    }

    // Запуск базовых воркеров
    for (int i = 1; i <= MIN_WORKERS; i++)
    {
        int id = i;
        _ = Task.Run(() => RunUdpWorkerAsync(id));
    }

    // 5. Адаптивный супервизор: динамически масштабирует воркеры до 100 штук при CPU < 80%
    var supervisorTask = Task.Run(async () =>
    {
        while (!cts.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(500, cts.Token);

                double currentCpu = ServerMetrics.CpuUsagePercent;
                int queueCount = toUdpChannel.Reader.Count;
                int currentLimit = Volatile.Read(ref targetUdpWorkers);

                if (currentCpu > 80.0)
                {
                    // ⚠️ Перегрузка CPU (>80%): плавно снижаем количество воркеров
                    if (currentLimit > MIN_WORKERS)
                    {
                        int newLimit = Math.Max(MIN_WORKERS, currentLimit - 5);
                        Volatile.Write(ref targetUdpWorkers, newLimit);
                    }
                }
                else if (currentCpu < 75.0 && queueCount > 0)
                {
                    // 🚀 Процессор свободен (<75%) и в очереди есть пакеты: масштабируем вверх
                    if (currentLimit < MAX_WORKERS)
                    {
                        int boost = Math.Min(10, queueCount + 1);
                        int newLimit = Math.Min(MAX_WORKERS, currentLimit + boost);
                        Volatile.Write(ref targetUdpWorkers, newLimit);

                        for (int i = currentLimit + 1; i <= newLimit; i++)
                        {
                            int id = i;
                            _ = Task.Run(() => RunUdpWorkerAsync(id));
                        }
                    }
                }
                else if (queueCount == 0 && currentLimit > 4)
                {
                    // Очередь пуста (затишье): плавно возвращаемся к базовому количеству
                    Volatile.Write(ref targetUdpWorkers, Math.Max(4, currentLimit - 2));
                }
            }
            catch (OperationCanceledException) { break; }
            catch { }
        }
    });

    // Ожидание завершения работы
    await Task.WhenAny(wsReceiveTask, udpReceiveTask);
    cts.Cancel();

    toWsChannel.Writer.TryComplete();
    toUdpChannel.Writer.TryComplete();

    // Очистка оставшихся буферов в очередях
    while (toWsChannel.Reader.TryRead(out var p)) ArrayPool<byte>.Shared.Return(p.Buffer);
    while (toUdpChannel.Reader.TryRead(out var p)) ArrayPool<byte>.Shared.Return(p.Buffer);

    Interlocked.Decrement(ref ServerMetrics.ActiveRelays);

    if (webSocket.State == WebSocketState.Open)
    {
        try
        {
            await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closed", CancellationToken.None);
        }
        catch { }
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

public static class ServerMetrics
{
    private static TimeSpan _lastCpuTime;
    private static DateTime _lastSampleTime = DateTime.UtcNow;
    public static double CpuUsagePercent { get; private set; }
    public static long MemoryMb { get; private set; }
    public static DateTime StartTime { get; } = DateTime.UtcNow;
    public static long TotalPacketsIn;
    public static long TotalPacketsOut;
    public static long PacketsInPerSec { get; private set; }
    public static long PacketsOutPerSec { get; private set; }
    public static int ActiveRelays;
    public static int TotalActiveWorkers;

    private static long _lastPacketsIn;
    private static long _lastPacketsOut;

    static ServerMetrics()
    {
        var proc = Process.GetCurrentProcess();
        _lastCpuTime = proc.TotalProcessorTime;
        _lastSampleTime = DateTime.UtcNow;

        _ = Task.Run(async () =>
        {
            while (true)
            {
                await Task.Delay(1000);
                try
                {
                    var now = DateTime.UtcNow;
                    var curCpu = proc.TotalProcessorTime;
                    var timePassed = (now - _lastSampleTime).TotalMilliseconds;

                    if (timePassed > 0)
                    {
                        var cpuUsed = (curCpu - _lastCpuTime).TotalMilliseconds;
                        var totalAvail = timePassed * Environment.ProcessorCount;
                        CpuUsagePercent = Math.Clamp(Math.Round((cpuUsed / totalAvail) * 100.0, 1), 0.0, 100.0);
                    }

                    _lastCpuTime = curCpu;
                    _lastSampleTime = now;
                    proc.Refresh();
                    MemoryMb = proc.WorkingSet64 / (1024 * 1024);

                    long curIn = Interlocked.Read(ref TotalPacketsIn);
                    long curOut = Interlocked.Read(ref TotalPacketsOut);

                    PacketsInPerSec = Math.Max(0, curIn - _lastPacketsIn);
                    PacketsOutPerSec = Math.Max(0, curOut - _lastPacketsOut);

                    _lastPacketsIn = curIn;
                    _lastPacketsOut = curOut;
                }
                catch { }
            }
        });
    }
}