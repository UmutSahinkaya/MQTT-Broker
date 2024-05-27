using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Data;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using MQTTnet.Client.Publishing;

namespace MQTT.Publisher
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            var factory = new MqttFactory();
            var mqttClient = factory.CreateMqttClient();
            //var data = LongReadOut.Message;
            var data = "test";
            const int deviceCount = 1000; 
            string topicSharedLong = Topic.topicLong;
            var options = new MqttClientOptionsBuilder()
                .WithClientId("PublisherClient")
                .WithTcpServer("localhost", 1883)
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .WithCredentials("test", "test")
                .WithCleanSession()
                .Build();

            // 2 saniye gecikme ekleyin
            await Task.Delay(2000);

            mqttClient.UseConnectedHandler(async e =>
            {
                Console.WriteLine("Publisher connected successfully.");

                for (int index = 0; index < deviceCount; index++)
                {
                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic(topicSharedLong)
                        .WithPayload($"{data} - {index + 1}")
                        .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build();

                    try
                    {
                        var result = await mqttClient.PublishAsync(message);
                        if (result.ReasonCode == MqttClientPublishReasonCode.Success)
                        {
                            Console.WriteLine($"Mesaj başarıyla yayımlandı: {index + 1}");
                        }
                        else
                        {
                            Console.WriteLine($"Mesaj yayımlanırken hata oluştu: {result.ReasonCode}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Mesaj yayınlama sırasında bir hata oluştu: {ex.Message}");
                    }

                }

                Console.WriteLine($"Tüm mesajlar yayımlandı: {deviceCount} cihazdan");
            });

            mqttClient.UseDisconnectedHandler(async e =>
            {
                Console.WriteLine("Publisher disconnected.");
                if (e.Exception != null)
                {
                    Console.WriteLine($"Bağlantı kesilme nedeni: {e.Exception.Message}");
                }
                try
                {
                    await mqttClient.ConnectAsync(options, CancellationToken.None); // Yeniden bağlanmayı dene
                }
                catch
                {
                    Console.WriteLine("### RECONNECTING FAILED ###");
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
