using System;
using System.Threading;
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
        private static IMqttClient mqttClient;
        private static string topic = Topic.topicLoadProfile; 
        private static Timer timer;
        
        public static async Task Main(string[] args)
        {
            var factory = new MqttFactory();
            mqttClient = factory.CreateMqttClient();

            var options = new MqttClientOptionsBuilder()
                .WithClientId("Publisher1Client")
                .WithTcpServer("localhost", 1883)
                .Build();

            mqttClient.UseConnectedHandler(async e =>
            {
                Console.WriteLine("Publisher connected successfully.");
                StartTimer();
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

        private static void StartTimer()
        {
            timer = new Timer(SendMessage, null, TimeSpan.Zero, TimeSpan.FromMinutes(15));
        }

        private static async void SendMessage(object state)
        {
            var data = new DataModel();
            

            var messageReadout = new MqttApplicationMessageBuilder()
                    .WithTopic(topic)
                    .WithPayload(data.ToJson())
                    .WithAtLeastOnceQoS()
                    .Build();

            try
            {
                var currentTime = DateTime.Now.ToString("HH:mm");
                var result = await mqttClient.PublishAsync(messageReadout);
                if (result.ReasonCode == MqttClientPublishReasonCode.Success)
                {
                    Console.WriteLine($"Mesaj başarıyla yayımlandı - Saat: {currentTime}");
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
    }
}
