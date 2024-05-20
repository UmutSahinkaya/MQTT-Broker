using MQTTnet.Client.Options;
using MQTTnet;
using MQTTnet.Client;

namespace MQTT.Publisher;
class Program
{
    public static async Task Main(string[] args)
    {
        var factory = new MqttFactory();
        var mqttClient = factory.CreateMqttClient();

        var options = new MqttClientOptionsBuilder()
            .WithClientId("PublisherClient")
            .WithTcpServer("localhost", 1883) // MQTT broker adresi ve portu
            .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
            .Build();

        mqttClient.UseConnectedHandler(async e =>
        {
            Console.WriteLine("Publisher connected successfully.");

            // Mesaj yayınlama
            var message = new MqttApplicationMessageBuilder()
                .WithTopic("shared/topic")
                .WithPayload("Hello Shared Subscription")
                .WithExactlyOnceQoS()
                .WithRetainFlag(false)
                .Build();

            await mqttClient.PublishAsync(message, CancellationToken.None);
        });

        await mqttClient.ConnectAsync(options, CancellationToken.None);

        Console.WriteLine("Press any key to exit.");
        Console.ReadLine();

        await mqttClient.DisconnectAsync();
    }
}