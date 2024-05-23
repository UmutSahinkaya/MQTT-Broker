using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;
using MQTTnet;
using MQTTnet.Client;
using Data;
using MQTTnet.Client.Options;

namespace MQTT.Subscriber3
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var factory = new MqttFactory();
            var mqttClient = factory.CreateMqttClient();
            string topicSharedLong = Topic.topicSharedLong;



            var options = new MqttClientOptionsBuilder()
                .WithClientId("Subscriber3Client")
                .WithTcpServer("localhost", 1883) // MQTT broker adresi ve portu
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .Build();

            // MongoDB'ye bağlan
            var client = new MongoClient("mongodb://localhost:27017");
            var database = client.GetDatabase("MyDatabase");
            var collection = database.GetCollection<BsonDocument>("LongReadCollection");

            mqttClient.UseConnectedHandler(async e =>
            {
                Console.WriteLine("Subscriber connected successfully.");

                // Shared Subscription ile abone olma
                await mqttClient.SubscribeAsync(new MqttTopicFilterBuilder()
                    .WithTopic(topicSharedLong)
                    .WithAtLeastOnceQoS()
                    .Build());

                Console.WriteLine($"Subscribed to {topicSharedLong} with shared subscription.");
            });

            mqttClient.UseDisconnectedHandler(e =>
            {
                Console.WriteLine("Subscriber 3 disconnected.");
                if (e.Exception != null)
                {
                    Console.WriteLine($"Bağlantı kesilme nedeni: {e.Exception.Message}");
                }
            });

            mqttClient.UseApplicationMessageReceivedHandler(async e =>
            {
                try
                {
                    string topic = e.ApplicationMessage.Topic;
                    string payload = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);
                    var document = new BsonDocument
                            {
                                { "Message", payload }
                            };
                    if (topic == topicSharedLong)
                    {
                        await collection.InsertOneAsync(document);
                        Console.WriteLine($"{topicSharedLong} topic'ine ait veri MongoDB'ye kaydedildi.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Bir hata ile karşılaşıldı: {ex.Message}");
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