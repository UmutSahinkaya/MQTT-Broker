using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;

namespace MQTT.Subscriber
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

            // MongoDB'ye bağlan
            var client = new MongoClient("mongodb://localhost:27017");
            var database = client.GetDatabase("MyDatabase");
            var collection = database.GetCollection<BsonDocument>("LongReadCollection");

            mqttClient.UseConnectedHandler(async e =>
            {
                Console.WriteLine("Subscriber connected successfully.");

                // Shared Subscription ile abone olma
                await mqttClient.SubscribeAsync(new MqttTopicFilterBuilder()
                    .WithTopic("$share/group1/topic/test") // Topic adı
                    .Build());

                Console.WriteLine("Subscribed to $share/group1/topic/test with shared subscription.");
            });

            mqttClient.UseDisconnectedHandler(e =>
            {
                Console.WriteLine("Subscriber disconnected.");
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

                    switch (topic)
                    {
                        case "$share/group1/topic/test":
                            var document = new BsonDocument
                            {
                                { "Message", payload }
                            };
                            await collection.InsertOneAsync(document);
                            Console.WriteLine("topic/test topic'ine ait veri MongoDB'ye kaydedildi.");
                            break;
                        case "/topic/test":
                             document = new BsonDocument
                            {
                                { "Message", payload }
                            };
                            await collection.InsertOneAsync(document);
                            Console.WriteLine("topic/test topic'ine ait veri MongoDB'ye kaydedildi.");
                            break;
                        case "topic/test":
                             document = new BsonDocument
                            {
                                { "Message", payload }
                            };
                            await collection.InsertOneAsync(document);
                            Console.WriteLine("topic/test topic'ine ait veri MongoDB'ye kaydedildi.");
                            break;
                        default:
                            Console.WriteLine("Böyle bir Topic YOK.");
                            break;
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
