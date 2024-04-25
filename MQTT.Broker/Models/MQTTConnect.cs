using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MQTT.Broker.Models;

public partial class MQTTConnect
{
    public int Id { get; set; }
    public string ClientID { get; set; }
    public Nullable<bool> SubscribeState { get; set; }
    public Nullable<bool> ConnectedState { get; set; }
    public string QosLevel { get; set; }
    public string Topic { get; set; }
    public string Endpoint { get; set; }
    public Nullable<System.DateTime> CreateDate { get; set; }
    public Nullable<System.DateTime> UpdateDate { get; set; }
}
