using System;
using System.Configuration;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Management.EventHub.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace IoTEHFunctionApp
{
    public class Function1
    {
        private readonly ILogger _logger;

        static EventHubProducerClient producerClient;

        public Function1(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<Function1>();
        }

        [Function("FunctionIoTEH")]
        //[EventHubOutput("%OutputEHName%", Connection = "OutputEHConnectionString")]
        public async Task Run([EventHubTrigger("%InputIoTHubEHCompatibleName%", Connection = "InputIoTHubConnectionString", ConsumerGroup = "%InputIoTHubConsumerGroup%")] string[] input)
        {
            _logger.LogInformation($"First Event Hubs triggered message: {input[0]}");

            

            var ehConn = Environment.GetEnvironmentVariable("OutputEHConnectionString", EnvironmentVariableTarget.Process);
            var ehName = Environment.GetEnvironmentVariable("OutputEHName", EnvironmentVariableTarget.Process);

            if (producerClient == null)
            {
                producerClient = new EventHubProducerClient(ehConn, ehName);
            }


            IList<string> adxMessages = new List<string>();

            EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

            try
            {
                var transformedJson = TransformPayload(input[0]);
                foreach(var dp in transformedJson)
                {
                    if (!eventBatch.TryAdd(new Azure.Messaging.EventHubs.EventData(Encoding.UTF8.GetBytes(dp))))
                    {
                        await producerClient.SendAsync(eventBatch);
                        eventBatch = await producerClient.CreateBatchAsync();
                    }
                }

                if (eventBatch.Count > 0)
                {
                    await producerClient.SendAsync(eventBatch);
                }
            }
            catch (Exception ex)
            {
                var x = ErrorPayload(input[0], ex.ToString());
                Console.WriteLine($"Error message: {x}");
            }
        }

        private string ErrorPayload(string payload, string error)
        {
            var message = new DataPoint()
            {
                Tag = "error",
                Timestamp = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                Value = payload,
                Server = error,
                TransformedTimestamp = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
            };

            var options = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            };


            var jsonString = JsonSerializer.Serialize<DataPoint>(message, options);
            return jsonString;
        }

        private string[] TransformPayload(string payload)
        {
            IList<string> dataPoints = new List<string>();

            var opc = JsonNode.Parse(payload);

            if (opc is JsonArray)
            {
                foreach(var item in ((JsonArray)opc))
                {
                    var dpjs = OPCToDatapoint(item);
                    dataPoints.Add(dpjs);
                }
            }
            else
            {
                var dpjs = OPCToDatapoint(opc);
                dataPoints.Add(dpjs);
            }

            return dataPoints.ToArray<string>();
        }


        private string OPCToDatapoint(JsonNode opc)
        {
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

            var options = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            };

            var jsonString = JsonSerializer.Serialize<DataPoint>(message, options);
            return jsonString;
        }
    }
}
