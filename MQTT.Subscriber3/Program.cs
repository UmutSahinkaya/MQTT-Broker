using System;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MQTTnet;
using MQTTnet.Client;

namespace MQTT.Subscriber3
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var factory = new MqttFactory();
            var mqttClient = factory.CreateMqttClient();

            var options = new MqttClientOptionsBuilder()
                .WithClientId("Subscriber3Client")
                .WithTcpServer("localhost", 1883) // MQTT broker adresi ve portu
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .Build();

            // MongoDB'ye bağlan
            var client = new MongoClient("mongodb://localhost:27017");
            var database = client.GetDatabase("MyDatabase");
            var collection = database.GetCollection<BsonDocument>("LongReadCollection");

            mqttClient.ConnectedAsync += async e =>
            {
                Console.WriteLine("Subscriber3 connected successfully.");

                // Shared Subscription ile abone olma
                await mqttClient.SubscribeAsync(new MqttTopicFilterBuilder()
                    .WithTopic("$share/group1/topic/test") // Topic adı
                    .WithExactlyOnceQoS()
                    .Build());

                Console.WriteLine("Subscribed to $share/group1/topic/test with shared subscription.");
            };

            mqttClient.DisconnectedAsync += e =>
            {
                Console.WriteLine("Subscriber3 disconnected.");
                if (e.Exception != null)
                {
                    Console.WriteLine($"Bağlantı kesilme nedeni: {e.Exception.Message}");
                }
                return Task.CompletedTask;
            };

            mqttClient.ApplicationMessageReceivedAsync += async e =>
            {
                try
                {
                    string topic = e.ApplicationMessage.Topic;
                    string payload = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);

                    if (topic == "$share/group1/topic/test")
                    {
                        var document = new BsonDocument
                        {
                            { "Message", payload }
                        };
                        await collection.InsertOneAsync(document);
                        Console.WriteLine(@"$share/group1/topic/test topic'ine ait veri MongoDB'ye kaydedildi.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Bir hata ile karşılaşıldı: {ex.Message}");
                }
            };

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
