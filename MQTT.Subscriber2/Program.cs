using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;
using MQTTnet;
using MQTTnet.Client;
using Data;
using MQTTnet.Client.Options;

namespace MQTT.Subscriber2
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var factory = new MqttFactory();
            var mqttClient = factory.CreateMqttClient();
            string longTopic = Topic.topicLong;
            string shortTopic = Topic.topicShort;
            string topicSharedLong = Topic.topicSharedLong;



            var options = new MqttClientOptionsBuilder()
                .WithClientId("Subscriber2Client")
                .WithTcpServer("localhost", 1883) // MQTT broker adresi ve portu
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .Build();

            // MongoDB'ye bağlan
            var client = new MongoClient("mongodb://localhost:27017");
            var database = client.GetDatabase("MyDatabase");
            var collection = database.GetCollection<BsonDocument>("LongReadCollection");

            mqttClient.UseConnectedHandler(async e =>
            {
                Console.WriteLine("Subscriber2 connected successfully.");

                // Shared Subscription ile abone olma
                await mqttClient.SubscribeAsync(new MqttTopicFilterBuilder()
                    .WithTopic(topicSharedLong)
                    .WithExactlyOnceQoS()
                    .Build());

                Console.WriteLine("Subscribed to $share/group1/topic/test with shared subscription.");
            });

            mqttClient.UseDisconnectedHandler(e =>
            {
                Console.WriteLine("Subscriber2 disconnected.");
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
                    if (topic == longTopic)
                    {
                        await collection.InsertOneAsync(document);
                        Console.WriteLine($"{longTopic} topic'ine ait veri MongoDB'ye kaydedildi.");
                    }
                    else if (topic == shortTopic)
                    {
                        await collection.InsertOneAsync(document);
                        Console.WriteLine($"{shortTopic} topic'ine ait veri MongoDB'ye kaydedildi.");
                    }
                    else if (topic == topicSharedLong)
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