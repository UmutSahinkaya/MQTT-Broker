using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;
using MQTTnet;
using MQTTnet.Client;
using Data;
using MQTTnet.Client.Options;
using RabbitMQ.Client;

namespace MQTT.Subscriber
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var options = new MqttClientOptionsBuilder()
                .WithClientId("SubscriberClient")
                .WithTcpServer("localhost", 1883)
                .Build();

            var factory = new MqttFactory();
            var mqttClient = factory.CreateMqttClient();
            string topicSharedLong = Topic.topicShared2Long;
            var sayac = 0;
            string topicLoadProfile = Topic.topicLoadProfile;

            //Rabbit Mq ayarları
            var rabbitmqFactory = new ConnectionFactory();
            rabbitmqFactory.Uri = new Uri("amqps://idcmiqqv:ud3-AL5qsqzzNxSa5oaOT9LPFcZyo2NU@cow.rmq2.cloudamqp.com/idcmiqqv");
            using var connection = rabbitmqFactory.CreateConnection();
            var channel = connection.CreateModel();
            channel.QueueDeclare("RawData-queue", true, false, false);
            channel.QueueDeclare("LoadProfile-queue", true, false, false);

            //MongoDB'ye bağlan
            var client = new MongoClient("mongodb://localhost:27017");
            var database = client.GetDatabase("MyDatabase");
            var readoutCollection = database.GetCollection<BsonDocument>("ReadoutCollection");
            var loadProfileCollection = database.GetCollection<BsonDocument>("LoadProfileCollection");

            mqttClient.UseConnectedHandler(async e =>
            {
                Console.WriteLine("Subscriber2 connected successfully.");

                // Shared Subscription ile abone olma
                await mqttClient.SubscribeAsync(new MqttTopicFilterBuilder()
                    .WithTopic(topicSharedLong)
                    .WithExactlyOnceQoS()
                    .Build());
                //await mqttClient.SubscribeAsync(new MqttTopicFilterBuilder()
                //    .WithTopic(topicLoadProfile)
                //    .WithAtLeastOnceQoS()
                //    .Build());

                Console.WriteLine($"Subscribed to {topicSharedLong}.");
            });

            mqttClient.UseDisconnectedHandler(async e =>
            {
                Console.WriteLine("Subscriber disconnected.");
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
                    //if (topic == topicSharedLong)
                    // {
                    await readoutCollection.InsertOneAsync(document);
                    string message = document["_id"].ToString();
                    var messageBody = Encoding.UTF8.GetBytes(message);
                    channel.BasicPublish(string.Empty, "Readout-queue", null, messageBody);
                    Console.WriteLine($"{topicSharedLong} topic'ine ait veri {payload}");
                    //}
                    //}else if (topic == topicLoadProfile)
                    //{
                    //    await loadProfileCollection.InsertOneAsync(document);
                    //    string message = document["_id"].ToString();
                    //    var messageBody = Encoding.UTF8.GetBytes(message);
                    //    channel.BasicPublish(string.Empty, "LoadProfile-queue", null, messageBody);
                    //    Console.WriteLine($"{topicLoadProfile} topic'ine ait veri MongoDB'ye kaydedildi.document_ıd : {document["_id"]}");
                    //}
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