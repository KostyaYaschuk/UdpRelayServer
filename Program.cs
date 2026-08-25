using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
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

app.MapGet("/", () => "High-Performance Zero-Jitter UDP Relay Server is Running!");

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

    int initialBatchMs = 2;
    if (int.TryParse(context.Request.Query["batchMs"], out int qBatchMs))
    {
        initialBatchMs = Math.Clamp(qBatchMs, 0, 1000);
    }

    int currentBatchDelayMs = initialBatchMs;

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

    var toWsChannel = Channel.CreateUnbounded<RentedPacket>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
    var toUdpChannel = Channel.CreateUnbounded<RentedPacket>(new UnboundedChannelOptions { SingleReader = false, SingleWriter = true });

    const int MIN_WORKERS = 2;
    const int MAX_WORKERS = 100;
    int targetUdpWorkers = MIN_WORKERS;

    // 1. Чтение UDP от Discord
    var udpReceiveTask = Task.Run(async () =>
    {
        var remoteEp = new IPEndPoint(IPAddress.Any, 0);
        while (!cts.IsCancellationRequested)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(65536);
            try
            {
                var result = await udpClient.Client.ReceiveFromAsync(new ArraySegment<byte>(buffer, 6, 65530), SocketFlags.None, remoteEp);
                long arrivalTimestamp = Stopwatch.GetTimestamp();
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

                if (!toWsChannel.Writer.TryWrite(new RentedPacket(buffer, totalLen, null, arrivalTimestamp)))
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
            catch (OperationCanceledException) { ArrayPool<byte>.Shared.Return(buffer); break; }
            catch { ArrayPool<byte>.Shared.Return(buffer); }
        }
    });

    // 2. Отправка в WebSocket с Fast-Path для пакетов подключения (< 120 байт)
    var wsSendTask = Task.Run(async () =>
    {
        var reader = toWsChannel.Reader;
        var batchBuffer = ArrayPool<byte>.Shared.Rent(65536);
        var packetList = new List<RentedPacket>(32);

        try
        {
            while (await reader.WaitToReadAsync(CancellationToken.None))
            {
                if (cts.IsCancellationRequested) break;

                packetList.Clear();

                if (!reader.TryRead(out var firstPacket))
                    continue;

                packetList.Add(firstPacket);

                int batchDelay = Volatile.Read(ref currentBatchDelayMs);

                // Fast-Path: если пакет одиночный или служебный (<120 байт, IP Discovery) — шлем мгновенно без задержек!
                bool isHandshakePacket = firstPacket.Length <= 120;

                if (batchDelay > 0 && !isHandshakePacket)
                {
                    var sw = Stopwatch.StartNew();
                    while (packetList.Count < 32 && sw.ElapsedMilliseconds < batchDelay)
                    {
                        if (reader.TryRead(out var nextPacket))
                        {
                            packetList.Add(nextPacket);
                            if (nextPacket.Length <= 120) break; // Служебный пакет — сразу закрываем пачку и шлем
                        }
                        else
                        {
                            await Task.Delay(1, cts.Token).ConfigureAwait(false);
                        }
                    }
                }
                else
                {
                    while (packetList.Count < 32 && reader.TryRead(out var nextPacket))
                    {
                        packetList.Add(nextPacket);
                    }
                }

                int totalBatchLen = 0;
                long freq = Stopwatch.Frequency;

                for (int i = 0; i < packetList.Count; i++)
                {
                    var p = packetList[i];
                    uint deltaUs = 0;

                    if (i > 0 && !isHandshakePacket)
                    {
                        long elapsedTicks = p.ArrivalTimestamp - packetList[i - 1].ArrivalTimestamp;
                        if (elapsedTicks > 0)
                        {
                            deltaUs = (uint)Math.Clamp((elapsedTicks * 1_000_000) / freq, 0, 500_000);
                        }
                    }

                    ushort subPacketLen = (ushort)(4 + p.Length);
                    if (totalBatchLen + 2 + subPacketLen > batchBuffer.Length)
                        break;

                    batchBuffer[totalBatchLen] = (byte)(subPacketLen >> 8);
                    batchBuffer[totalBatchLen + 1] = (byte)(subPacketLen & 0xFF);
                    totalBatchLen += 2;

                    batchBuffer[totalBatchLen] = (byte)(deltaUs >> 24);
                    batchBuffer[totalBatchLen + 1] = (byte)((deltaUs >> 16) & 0xFF);
                    batchBuffer[totalBatchLen + 2] = (byte)((deltaUs >> 8) & 0xFF);
                    batchBuffer[totalBatchLen + 3] = (byte)(deltaUs & 0xFF);
                    totalBatchLen += 4;

                    Buffer.BlockCopy(p.Buffer, 0, batchBuffer, totalBatchLen, p.Length);
                    totalBatchLen += p.Length;
                }

                foreach (var p in packetList) ArrayPool<byte>.Shared.Return(p.Buffer);

                if (totalBatchLen > 0 && webSocket.State == WebSocketState.Open && !cts.IsCancellationRequested)
                {
                    await webSocket.SendAsync(new ReadOnlyMemory<byte>(batchBuffer, 0, totalBatchLen), WebSocketMessageType.Binary, true, cts.Token);
                }
            }
        }
        catch { }
        finally
        {
            ArrayPool<byte>.Shared.Return(batchBuffer);
            foreach (var p in packetList) ArrayPool<byte>.Shared.Return(p.Buffer);
        }
    });

    // 3. Прием из WebSocket
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

                if (result.MessageType == WebSocketMessageType.Text)
                {
                    var text = Encoding.UTF8.GetString(buffer, 0, result.Count);
                    if (int.TryParse(text.Trim(), out int newBatchMs))
                    {
                        Volatile.Write(ref currentBatchDelayMs, Math.Clamp(newBatchMs, 0, 1000));
                    }
                    ArrayPool<byte>.Shared.Return(buffer);
                    continue;
                }

                if (result.MessageType == WebSocketMessageType.Binary && result.Count >= 12)
                {
                    int offset = 0;
                    while (offset + 2 <= result.Count)
                    {
                        ushort pLen = (ushort)((buffer[offset] << 8) | buffer[offset + 1]);
                        offset += 2;

                        if (pLen < 10 || offset + pLen > result.Count)
                            break;

                        uint deltaUs = (uint)((buffer[offset] << 24) |
                                              (buffer[offset + 1] << 16) |
                                              (buffer[offset + 2] << 8) |
                                              buffer[offset + 3]);

                        var targetIp = new IPAddress(buffer.AsSpan(offset + 4, 4));
                        var targetPort = (ushort)((buffer[offset + 8] << 8) | buffer[offset + 9]);
                        var ep = new IPEndPoint(targetIp, targetPort);

                        int dataLen = pLen - 4;
                        byte[] subBuffer = ArrayPool<byte>.Shared.Rent(dataLen);
                        Buffer.BlockCopy(buffer, offset + 4, subBuffer, 0, dataLen);

                        Interlocked.Increment(ref ServerMetrics.TotalPacketsOut);

                        if (!toUdpChannel.Writer.TryWrite(new RentedPacket(subBuffer, dataLen, ep, 0, deltaUs)))
                        {
                            ArrayPool<byte>.Shared.Return(subBuffer);
                        }

                        offset += pLen;
                    }
                }

                ArrayPool<byte>.Shared.Return(buffer);
            }
            catch { ArrayPool<byte>.Shared.Return(buffer); break; }
        }
    });

    // 4. Отправка UDP в Discord
    async Task RunUdpWorkerAsync(int workerId)
    {
        Interlocked.Increment(ref ServerMetrics.TotalActiveWorkers);
        try
        {
            var reader = toUdpChannel.Reader;
            while (!cts.IsCancellationRequested)
            {
                if (workerId > Volatile.Read(ref targetUdpWorkers))
                    break;

                if (!await reader.WaitToReadAsync(CancellationToken.None))
                    break;

                while (reader.TryRead(out var packet))
                {
                    try
                    {
                        if (packet.EndPoint != null && !cts.IsCancellationRequested)
                        {
                            // Пейсинг воспроизводится только для основного потока данных (не для служебных пакетов подключения)
                            if (packet.DeltaMicroseconds > 0 && packet.Length > 120)
                            {
                                await PreciseDelayAsync(packet.DeltaMicroseconds, cts.Token);
                            }

                            var data = new ReadOnlyMemory<byte>(packet.Buffer, 6, packet.Length - 6);
                            await udpClient.SendAsync(data, packet.EndPoint, cts.Token);
                        }
                    }
                    catch { }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(packet.Buffer);
                    }

                    if (workerId > Volatile.Read(ref targetUdpWorkers))
                        return;
                }
            }
        }
        finally
        {
            Interlocked.Decrement(ref ServerMetrics.TotalActiveWorkers);
        }
    }

    for (int i = 1; i <= MIN_WORKERS; i++)
    {
        int id = i;
        _ = Task.Run(() => RunUdpWorkerAsync(id));
    }

    // 5. Супервизор нагрузки
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
                    if (currentLimit > MIN_WORKERS)
                    {
                        Volatile.Write(ref targetUdpWorkers, Math.Max(MIN_WORKERS, currentLimit - 5));
                    }
                }
                else if (currentCpu < 75.0 && queueCount > 0)
                {
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
                    Volatile.Write(ref targetUdpWorkers, Math.Max(4, currentLimit - 2));
                }
            }
            catch (OperationCanceledException) { break; }
            catch { }
        }
    });

    await Task.WhenAny(wsReceiveTask, udpReceiveTask);
    cts.Cancel();

    toWsChannel.Writer.TryComplete();
    toUdpChannel.Writer.TryComplete();

    while (toWsChannel.Reader.TryRead(out var p)) ArrayPool<byte>.Shared.Return(p.Buffer);
    while (toUdpChannel.Reader.TryRead(out var p)) ArrayPool<byte>.Shared.Return(p.Buffer);

    Interlocked.Decrement(ref ServerMetrics.ActiveRelays);

    if (webSocket.State == WebSocketState.Open)
    {
        try { await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closed", CancellationToken.None); } catch { }
    }
});

app.Run();

static async Task PreciseDelayAsync(uint microseconds, CancellationToken token)
{
    if (microseconds == 0) return;
    microseconds = Math.Min(microseconds, 500_000);

    long freq = Stopwatch.Frequency;
    long targetTicks = Stopwatch.GetTimestamp() + (long)(microseconds * (double)freq / 1_000_000.0);

    if (microseconds >= 2000)
    {
        int msToWait = (int)(microseconds / 1000) - 1;
        if (msToWait > 0)
        {
            await Task.Delay(msToWait, token).ConfigureAwait(false);
        }
    }

    while (Stopwatch.GetTimestamp() < targetTicks)
    {
        if (token.IsCancellationRequested) break;
        Thread.SpinWait(10);
    }
}

public readonly struct RentedPacket
{
    public readonly byte[] Buffer;
    public readonly int Length;
    public readonly IPEndPoint? EndPoint;
    public readonly long ArrivalTimestamp;
    public readonly uint DeltaMicroseconds;

    public RentedPacket(byte[] buffer, int length, IPEndPoint? endPoint, long arrivalTimestamp = 0, uint deltaMicroseconds = 0)
    {
        Buffer = buffer;
        Length = length;
        EndPoint = endPoint;
        ArrivalTimestamp = arrivalTimestamp;
        DeltaMicroseconds = deltaMicroseconds;
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