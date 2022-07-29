using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTEHFunctionApp
{
    public class DataPoint
    {
        public string Tag { get; set; }
        public string Timestamp { get; set; }
        public string Value { get; set; }
        public string Server { get; set; }
        public string TransformedTimestamp { get; set; }
    }
}
