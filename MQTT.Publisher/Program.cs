using System;
using System.Collections.Generic;
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
            const int deviceCount = 100; 
            string sharedTopic = Topic.topicSharedLong;
            var options = new MqttClientOptionsBuilder()
                .WithClientId("PublisherClient")
                .WithTcpServer("localhost", 1883)
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .Build();

            // 2 saniye gecikme ekleyin
            await Task.Delay(2000);

            mqttClient.UseConnectedHandler(async e =>
            {
                Console.WriteLine("Publisher connected successfully.");

                for (int index = 0; index < deviceCount; index++)
                {
                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic(sharedTopic)
                        .WithPayload($"{data} - {index + 1}")
                        .WithExactlyOnceQoS()
                        .Build();

                    try
                    {
                        await mqttClient.PublishAsync(message);
                        Console.WriteLine($"Mesaj yayımlandı: {index + 1}"); // Mesajın yayımlandığını göster
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Mesaj yayınlama sırasında bir hata oluştu: {ex.Message}");
                    }
                }

                Console.WriteLine($"Tüm mesajlar yayımlandı: {deviceCount} cihazdan");
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
                await mqttClient.ConnectAsync(options);
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
