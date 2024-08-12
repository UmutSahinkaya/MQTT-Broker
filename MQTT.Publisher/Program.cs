using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using MQTTnet.Client.Publishing;
using Newtonsoft.Json;

namespace MQTT.Publisher
{
    class Program
    {
        private static IMqttClient mqttClient;
        private static string topic;
        private static string _clientId;
        private static Timer timer;
        private static JsonData data;
        private static string logFilePath = "mqtt_logs.txt"; // Log dosyasının yolunu belirleyin

        public static async Task Main(string[] args)
        {
            // Datas.json dosyasını oku ve verileri değişkenlere ata
            string jsonFilePath = "Datas.json";

            if (!File.Exists(jsonFilePath))
            {
                Console.WriteLine("Dosya bulunamadı: " + jsonFilePath);
                return;
            }

            string jsonContent = File.ReadAllText(jsonFilePath);
            data = JsonConvert.DeserializeObject<JsonData>(jsonContent);

            if (data == null)
            {
                Console.WriteLine("JSON verisi okunamadı veya hatalı.");
                return;
            }

            topic = data.Topic;
            _clientId = data.ClientId;
            var factory = new MqttFactory();
            mqttClient = factory.CreateMqttClient();

            var options = new MqttClientOptionsBuilder()
                .WithClientId($"{_clientId}")
                .WithTcpServer("92.45.116.60", 1883)
                .Build();

            mqttClient.UseConnectedHandler(async e =>
            {
                Console.WriteLine("Publisher connected successfully.");
                LogMessage("Publisher connected successfully.");
                StartTimer(data.Time);
            });

            mqttClient.UseDisconnectedHandler(async e =>
            {
                Console.WriteLine("Publisher disconnected.");
                LogMessage("Publisher disconnected.");

                if (e.Exception != null)
                {
                    Console.WriteLine($"Bağlantı kesilme nedeni: {e.Exception.Message}");
                    LogMessage($"Bağlantı kesilme nedeni: {e.Exception.Message}");
                }

                try
                {
                    await mqttClient.ConnectAsync(options, CancellationToken.None); // Yeniden bağlanmayı dene
                    LogMessage("Reconnecting to the MQTT broker.");
                }
                catch (Exception reconnectEx)
                {
                    Console.WriteLine($"### RECONNECTING FAILED: {reconnectEx.Message} ###");
                    LogMessage($"### RECONNECTING FAILED: {reconnectEx.Message} ###");
                }
            });

            try
            {
                await mqttClient.ConnectAsync(options, CancellationToken.None);
                LogMessage("Connected to MQTT broker successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Bağlantı sırasında bir hata oluştu: {ex.Message}");
                LogMessage($"Connection error: {ex.Message}");
                return;
            }

            Console.WriteLine("Press any key to exit.");
            Console.ReadLine();

            await mqttClient.DisconnectAsync();
            LogMessage("Publisher disconnected manually.");
        }

        private static void StartTimer(int time)
        {
            // Timer'ı belirtilen dakika aralıklarla çalışacak şekilde ayarlayın
            timer = new Timer(SendMessage, null, TimeSpan.Zero, TimeSpan.FromMinutes(time));
        }

        private static async void SendMessage(object state)
        {
            // SerialNumber'ı da mesaj yüküne dahil edin
            string selectedReadout;
            string selectedTopic;

            // topiğe göre veriyi seç
            if (data.Topic == "topicReadout")
            {
                selectedTopic = data.topicReadout;
            }
            else if (data.Topic == "topicLoadProfile")
            {
                selectedTopic = data.topicLoadProfile;
            }
            else
            {
                Console.WriteLine("Geçersiz topic değeri. Mesaj gönderilemedi.");
                LogMessage("Geçersiz topic değeri. Mesaj gönderilemedi.");
                return;
            }




            // Readout değerine göre veriyi seç
            if (data.Readout == "LongReadout")
            {
                selectedReadout = data.LongReadout;
            }
            else if (data.Readout == "LoadProfileMessage")
            {
                selectedReadout = data.LoadProfileMessage;
            }
            else
            {
                Console.WriteLine("Geçersiz Readout değeri. Mesaj gönderilemedi.");
                LogMessage("Geçersiz Readout değeri. Mesaj gönderilemedi.");
                return;
            }

            var payload = JsonConvert.SerializeObject(new
            {
                SerialNumber = data.SerialNumber,
                Message = selectedReadout
            });

            var messageReadout = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payload)
                .WithAtLeastOnceQoS()
                .Build();

            try
            {
                var currentTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"); // Gün ve saat bilgileri
                var result = await mqttClient.PublishAsync(messageReadout);

                if (result.ReasonCode == MqttClientPublishReasonCode.Success)
                {
                    Console.WriteLine($"Mesaj başarıyla YAYIMLANDI ");
                    LogMessage($"Mesaj başarıyla YAYIMLANDI");
                }
                else
                {
                    Console.WriteLine($"Mesaj yayımlanırken hata oluştu: {result.ReasonCode}");
                    LogMessage($"Mesaj yayımlanırken hata oluştu: {result.ReasonCode}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Mesaj yayınlama sırasında bir hata oluştu: {ex.Message}");
                LogMessage($"Mesaj yayınlama sırasında bir hata oluştu: {ex.Message}");
            }
        }

        // Log mesajını dosyaya yazan metod
        private static void LogMessage(string message)
        {
            try
            {
                // Log mesajına zaman damgası ekleyin
                string logEntry = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss}: {message}";

                // Log mesajını dosyaya ekleyin
                File.AppendAllText(logFilePath, logEntry + Environment.NewLine);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Log yazılamadı: {ex.Message}");
            }
        }

        // JSON verilerini okumak için bir sınıf tanımlayın
        public class JsonData
        {
            public string SerialNumber { get; set; }
            public string ClientId { get; set; }
            public string Topic { get; set; }
            public int Time { get; set; }
            public string topicLoadProfile { get; set; }
            public string topicReadout { get; set; }
            public string LongReadout { get; set; }
            public string LoadProfileMessage { get; set; } // LoadProfile verisi için alan
            public string Readout { get; set; } // Dışarıdan belirlenen Readout değeri
        }
    }
}
