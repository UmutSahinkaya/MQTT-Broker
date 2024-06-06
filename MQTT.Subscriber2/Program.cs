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
            var options = new MqttClientOptionsBuilder()
                .WithClientId("Subscriber2Client")
                .WithTcpServer("localhost", 1883)
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .Build();

            var factory = new MqttFactory();
            var mqttClient = factory.CreateMqttClient();
            string topicSharedLong = Topic.topicSharedLong;



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
                    .WithAtLeastOnceQoS()
                    .Build());

                Console.WriteLine($"Subscribed to {topicSharedLong} with shared subscription.");
            });

            mqttClient.UseDisconnectedHandler(async e =>
            {
                Console.WriteLine("Subscriber 2 disconnected.");
                if (e.Exception != null)
                {
                    Console.WriteLine($"Bağlantı kesilme nedeni: {e.Exception.Message}");
                }
                await Task.Delay(TimeSpan.FromSeconds(5)); // Bekleyin ve yeniden bağlanmayı deneyin
                await mqttClient.ConnectAsync(options, CancellationToken.None);
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
                        Console.WriteLine($"{topicSharedLong} topic'ine ait veri MongoDB'ye kaydedildi.document_ıd : {document["_id"]}");
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