FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["UdpRelayServer.csproj", "./"]
RUN dotnet restore "UdpRelayServer.csproj"
COPY . .
RUN dotnet publish "UdpRelayServer.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app
COPY --from=build /app/publish .

ENV DOTNET_USE_POLLING_FILE_WATCHER=true
ENV DOTNET_EnableDiagnostics=0

ENTRYPOINT ["dotnet", "UdpRelayServer.dll"]  