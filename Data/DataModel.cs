using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Data
{
    public  class DataModel
    {
        public string SerialNumber = "12345671"; // "123456789
        public string Message { get; set; } = Messages.LoadProfileMessage2;

        public string ToJson()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}
