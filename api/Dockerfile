FROM mcr.microsoft.com/dotnet/nightly/sdk:6.0 AS build
WORKDIR /src
COPY . .
RUN dotnet tool restore
RUN dotnet paket install
RUN dotnet restore
RUN dotnet publish -c "release" -o "/app" --no-restore

FROM mcr.microsoft.com/dotnet/nightly/aspnet:6.0
WORKDIR /app
COPY --from=build /app .
EXPOSE 80
ENTRYPOINT [ "./api" ]