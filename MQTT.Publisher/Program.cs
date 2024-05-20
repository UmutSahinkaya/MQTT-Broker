using System;
using System.Threading.Tasks;
using Data;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;

namespace MQTT.Publisher
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            var factory = new MqttFactory();
            var mqttClient = factory.CreateMqttClient();
            var data = LongReadOut.Message;
            const int sayac = 10000;

            var options = new MqttClientOptionsBuilder()
                .WithClientId("PublisherClient")
                .WithTcpServer("localhost", 1883) // MQTT broker adresi ve portu
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .Build();

            await Task.Delay(2000);
            mqttClient.UseConnectedHandler(async e =>
            {
                Console.WriteLine("Publisher connected successfully.");

                for (int index = 0; index < sayac; index++)
                {
                    var message = new MqttApplicationMessageBuilder()
                         .WithTopic("topic/test") // Topic adı
                         .WithPayload($"{data} - {index + 1}")
                         .WithAtLeastOnceQoS()
                         .WithRetainFlag(false)
                         .Build();
                    try
                    {
                        await mqttClient.PublishAsync(message, CancellationToken.None);
                        Console.WriteLine($"Mesaj yayımlandı: {index + 1} kez");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Mesaj yayınlama sırasında bir hata oluştu: {ex.Message}");
                    }
                }
            });

            mqttClient.UseDisconnectedHandler(e =>
            {
                Console.WriteLine("Publisher disconnected.");
                if (e.Exception != null)
                {
                    Console.WriteLine($"Bağlantı kesilme nedeni: {e.Exception.Message}");
                }
            });

            try
            {
                await mqttClient.ConnectAsync(options, CancellationToken.None);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Bağlantı sırasında bir hata oluştu: {ex.Message}");
                return;
            }

            Console.WriteLine("Press any key to exit.");
            Console.ReadLine();

            await mqttClient.DisconnectAsync();
        }
    }
}
