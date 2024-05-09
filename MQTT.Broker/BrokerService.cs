using MongoDB.Bson;
using MongoDB.Driver;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using MQTTnet.Protocol;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MQTT.Broker
{
    public class BrokerService
    {
        private readonly IMqttClient _mqttClient;

        public BrokerService()
        {
            var mqttFactory = new MqttFactory();
            _mqttClient = mqttFactory.CreateMqttClient();
        }

        public async Task Start()
        {
            var mqttClientOptions = new MqttClientOptionsBuilder()
                .WithTcpServer("localhost", 1883)
                .WithClientId("ClientSubscriber")
                .Build();

            // MongoDB'ye bağlan
            var client = new MongoClient("mongodb://localhost:27017");
            var database = client.GetDatabase("MyDatabase");
            var collection = database.GetCollection<BsonDocument>("MessageCollection");

            _mqttClient.UseApplicationMessageReceivedHandler(async e =>
            {
                try
                {
                    string topic = e.ApplicationMessage.Topic;
                    string payload = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);

                    switch (topic)
                    {
                        case "test/topic":
                            var document = new BsonDocument
                            {
                                { "Message", payload }
                            };
                            await collection.InsertOneAsync(document);
                            Console.WriteLine("test/topic topic'ine ait veri MongoDB'ye kaydedildi.");
                            break;
                        case "test/receive":
                            // Eğer bu topic'e özel işlemler yapılacaksa buraya ekleyebilirsiniz.
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

            await _mqttClient.ConnectAsync(mqttClientOptions);

            Console.WriteLine("MQTT istemcisi başlatıldı ve dinleme işlemi başlatıldı...");
        }
    }
}
