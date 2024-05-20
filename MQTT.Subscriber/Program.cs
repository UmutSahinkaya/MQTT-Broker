using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;

namespace MqttSubscriber
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var factory = new MqttFactory();
            var mqttClient = factory.CreateMqttClient();

            var options = new MqttClientOptionsBuilder()
                .WithClientId("SubscriberClient")
                .WithTcpServer("localhost", 1883) // MQTT broker adresi ve portu
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .Build();

            mqttClient.UseConnectedHandler(async e =>
            {
                Console.WriteLine("Subscriber connected successfully.");

                // Shared Subscription ile abone olma
                await mqttClient.SubscribeAsync(new MqttTopicFilterBuilder()
                    .WithTopic("$share/group1/shared/topic")
                    .Build());

                Console.WriteLine("Subscribed to $share/group1/shared/topic with shared subscription.");
            });

            mqttClient.UseDisconnectedHandler(e =>
            {
                Console.WriteLine("Subscriber disconnected.");
            });

            mqttClient.UseApplicationMessageReceivedHandler(e =>
            {
                var message = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);
                Console.WriteLine($"Received message: {message} from topic: {e.ApplicationMessage.Topic}");
            });

            await mqttClient.ConnectAsync(options, CancellationToken.None);

            Console.WriteLine("Press any key to exit.");
            Console.ReadLine();

            await mqttClient.DisconnectAsync();
        }
    }
}
