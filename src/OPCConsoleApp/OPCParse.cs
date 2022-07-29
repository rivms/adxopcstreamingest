using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace OPCConsoleApp
{
    internal class OPCParse
    {
        private string jsonText = @"{
    ""NodeId"": ""nsu = http://microsoft.com/Opc/OpcPlc/;s=FastUInt42"",
    ""EndpointUrl"": ""opc.tcp://aci-contoso-dsh53lk-plc1.australiaeast.azurecontainer.io:50000/"",
    ""Value"": {
                ""Value"": 645941,
        ""SourceTimestamp"": ""2022-07-20T08:29:09.1435197Z""
    }
        }";
        public void Run()
        {
            var opc = JsonNode.Parse(jsonText);


            var nodeId = (string)opc["NodeId"];
            var endpointUrl = (string)opc["EndpointUrl"];
            var opcValue = opc["Value"]["Value"].ToString();
            var sourceTimestamp = (string)opc["Value"]["SourceTimestamp"];

            var message = new DataPoint()
            {
                Tag = nodeId,
                Timestamp = sourceTimestamp,
                Value = opcValue,
                Server = endpointUrl,
                TransformedTimestamp = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
            };

            var jsonString = JsonSerializer.Serialize<DataPoint>(message);
            Console.WriteLine(jsonString);



        }
    }
}
