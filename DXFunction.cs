using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace Mizobata.Function
{
    public class DXFunction
    {
        private readonly ILogger<DXFunction> _logger;
        private readonly string? _connectionString;
        private readonly HttpClient _httpClient;
        private readonly string? _mlEndpoint;
        private readonly string? _mlApiKey;

        public DXFunction(ILogger<DXFunction> logger)
        {
            _logger = logger;
            _connectionString = Environment.GetEnvironmentVariable("ADO_NET_CONNECTION");
            _mlEndpoint = Environment.GetEnvironmentVariable("ML_ENDPOINT_URL");
            _mlApiKey = Environment.GetEnvironmentVariable("ML_API_KEY");

            if (string.IsNullOrEmpty(_connectionString) || string.IsNullOrEmpty(_mlEndpoint) || string.IsNullOrEmpty(_mlApiKey))
                throw new InvalidOperationException("Missing required environment variables.");

            _httpClient = new HttpClient();
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", _mlApiKey);
        }

        [Function(nameof(DXFunction))]
        public async Task Run([EventHubTrigger(
            eventHubName: "iothub-ehub-mizobataka-56011127-6e264844b5",
            Connection = "mizobatakagakunamespace_RootManageSharedAccessKey_EVENTHUB"
        )] EventData[] events)
        {
            foreach (EventData @event in events)
            {
                try
                {
                    string json = @event.EventBody.ToString();
                    var doc = JsonDocument.Parse(json);

                    if (!doc.RootElement.TryGetProperty("machine_id", out var machineIdElement) ||
                        !doc.RootElement.TryGetProperty("image_base64", out var imageElement)) {
                        _logger.LogWarning("Missing 'machine_id' or 'image_base64' in event data.");
                        continue;
                    }

                    string machineId = machineIdElement.GetString()!;
                    string imageBase64 = imageElement.GetString()!;

                    // ML 推論リクエストの作成
                    var mlRequestBody = new
                    {
                        image = imageBase64
                    };
                    var mlRequestJson = JsonSerializer.Serialize(mlRequestBody);
                    var content = new StringContent(mlRequestJson, Encoding.UTF8, "application/json");

                    // MLエンドポイントに送信
                    HttpResponseMessage response = await _httpClient.PostAsync(_mlEndpoint, content);
                    response.EnsureSuccessStatusCode();
                    string mlResponseJson = await response.Content.ReadAsStringAsync();

                    var mlResponse = JsonDocument.Parse(mlResponseJson);

                    if (!mlResponse.RootElement.TryGetProperty("predicted_class", out var classElement) || classElement.ValueKind != JsonValueKind.String) {
                        _logger.LogWarning("Invalid or missing 'predicted_class' in ML response.");
                        continue;
                    }
                    string predictedClass = classElement.GetString()!;
                    _logger.LogInformation("ML predicted_class for machine {machineId}: {predictedClass}", machineId, predictedClass);

                    if (!mlResponse.RootElement.TryGetProperty("result", out var resultElement) || resultElement.ValueKind != JsonValueKind.True && resultElement.ValueKind != JsonValueKind.False) {
                        _logger.LogWarning("Invalid or missing 'result' in ML response.");
                        continue;
                    }

                    bool isActivated = resultElement.GetBoolean();
                    _logger.LogInformation("ML result for machine {machineId}: {result}", machineId, isActivated);

                    if (predictedClass == "class1") {
                        using var conn = new SqlConnection(_connectionString);
                        conn.Open();
                        using var cmd = conn.CreateCommand();
                        cmd.CommandText = "INSERT INTO OperationLogs (machine_id, activate) VALUES (@machine_id, @activate)";
                        cmd.Parameters.AddWithValue("@machine_id", machineId);
                        cmd.Parameters.AddWithValue("@activate", isActivated ? "1" : "0");
                        cmd.ExecuteNonQuery();

                        _logger.LogInformation("Inserted into SQL DB for machine_id: {machineId}", machineId);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing EventHub message.");
                }
            }
        }
    }
}
