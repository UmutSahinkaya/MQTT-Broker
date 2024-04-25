﻿using MQTT.Broker.Models;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using MQTTnet.Server;
using Newtonsoft.Json;
using System.Text;
using System.Text.Json.Serialization;

namespace MQTT.Broker;

public class BrokerService
{
    IMqttServer _mqttServer;
    MqttServerOptionsBuilder _mqttServerOptionsBuilder;
    public BrokerService()
    {
        _mqttServerOptionsBuilder = new MqttServerOptionsBuilder()
            .WithConnectionValidator(c =>
            {
                var client = new MQTTConnect()
                {
                    ClientID = c.ClientId,
                    ConnectedState = true,
                    Endpoint = c.Endpoint,
                    QosLevel = string.Empty,
                    SubscribeState = false,
                    Topic = string.Empty,
                    CreateDate = DateTime.Now,
                    UpdateDate = DateTime.Now
                };

                Console.WriteLine($"{DateTime.UtcNow.ToString("HH:mm:ss")},Endpoint: {c.Endpoint}");
                if (c.Username == "MyBroker" && c.Password == "MyPassword")
                    c.ReasonCode = MqttConnectReasonCode.Success;
                else
                    c.ReasonCode = MqttConnectReasonCode.NotAuthorized;
            })
            .WithApplicationMessageInterceptor(async context =>
            {
                Console.WriteLine($"Id: {context.ClientId} ==> \ntopic: {context.ApplicationMessage.Topic} \nPayload ==> {Encoding.UTF8.GetString(context.ApplicationMessage.Payload)}");
            })
            .WithConnectionBacklog(1000)
            .WithDefaultEndpointBoundIPAddress(System.Net.IPAddress.Parse("127.0.0.1"))
            .WithDefaultEndpointPort(1884)
            .WithSubscriptionInterceptor(context =>
            {
                if (context.TopicFilter.Topic.StartsWith("test/topic") || context.TopicFilter.Topic.StartsWith("test/receive"))
                {
                    context.AcceptSubscription = true;
                }
                else
                {
                    context.AcceptSubscription = false;
                }
            });
        
    }
    
    public void Start()
    {
        _mqttServer= new MqttFactory().CreateMqttServer();
        _mqttServer.StartAsync(_mqttServerOptionsBuilder.Build());
        _mqttServer.UseApplicationMessageReceivedHandler(async e =>
        {
            try
            {
                string topic = e.ApplicationMessage.Topic;
                var payload = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);
                var data = JsonConvert.DeserializeObject<Datas>(payload);
                switch (topic)
                {
                    case "test/topic":
                        Console.WriteLine($"Name:{data.Name} \n Surname:{data.Surname} \n Birthday:{data.BirthDay}");
                        break;
                    case "test/receive":
                        Console.WriteLine($"Name:{data.Name} \n Surname:{data.Surname} \n Birthday:{data.BirthDay}");
                        break;
                    default: Console.WriteLine("Böyle bir Topic YOOK."); break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Bir hata ile karşılaşıldı.{ex}");
            }
        });
       
        Console.WriteLine($"Mqtt Broker oluşturuldu: Host: {_mqttServer.Options.DefaultEndpointOptions.BoundInterNetworkAddress} Port: {_mqttServer.Options.DefaultEndpointOptions.Port}");

        Task.Run(() => Thread.Sleep(Timeout.Infinite)).Wait();
    }
    public void Stop()
    {
        _mqttServer.StopAsync().Wait();
    }
}
