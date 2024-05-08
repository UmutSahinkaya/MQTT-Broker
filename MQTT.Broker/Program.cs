using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;

var factory = new MqttFactory();
var client = factory.CreateMqttClient();

var options = new MqttClientOptionsBuilder()
    .WithTcpServer("172.17.0.4", 1883) // Broker adresi ve portunu belirleyin
    .WithCredentials("guest", "guest")     // Kullanıcı adı ve parolayı belirleyin
    .Build();

await client.ConnectAsync(options);
