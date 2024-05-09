FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
USER app
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["MQTT.Publisher/MQTT.Publisher.csproj", "MQTT.Publisher/"]
COPY ["MQTT.Broker/MQTT.Broker.csproj", "MQTT.Broker/"]
RUN dotnet restore "./MQTT.Publisher/MQTT.Publisher.csproj"
COPY . .
WORKDIR "/src/MQTT.Publisher"
RUN dotnet build "./MQTT.Publisher.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./MQTT.Publisher.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=true

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "MQTT.Publisher.dll"]