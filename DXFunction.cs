using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;

namespace Mizobata.Function {
    public class DXFunction {
        private readonly ILogger<DXFunction> _logger;
        private readonly string? _connectionString;
        private readonly HttpClient _httpClient;
        private readonly string? _mlEndpoint;
        private readonly string? _mlApiKey;
        private readonly string? _blobConnectionString;

        public DXFunction(ILogger<DXFunction> logger) {
            _logger = logger;
            _connectionString = Environment.GetEnvironmentVariable("ADO_NET_CONNECTION");
            _mlEndpoint = Environment.GetEnvironmentVariable("ML_ENDPOINT_URL");
            _mlApiKey = Environment.GetEnvironmentVariable("ML_API_KEY");
            _blobConnectionString = Environment.GetEnvironmentVariable("BLOB_CONNECTION_STRING")!;

            if (string.IsNullOrEmpty(_connectionString) || string.IsNullOrEmpty(_mlEndpoint) || string.IsNullOrEmpty(_mlApiKey) || string.IsNullOrEmpty(_blobConnectionString)){
                throw new InvalidOperationException("Missing required environment variables.");
            }
            _httpClient = new HttpClient();
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", _mlApiKey);
        }

        [Function(nameof(DXFunction))]
        public async Task Run([EventHubTrigger(
            eventHubName: "iothub-ehub-mizobataka-56011127-6e264844b5",
            Connection = "mizobatakagakunamespace_RootManageSharedAccessKey_EVENTHUB"
        )] EventData[] events) {
            foreach (EventData @event in events) {
                try {
                    string json = @event.EventBody.ToString();
                    _logger.LogInformation("Raw event body (truncated): {json}", json.Substring(0, Math.Min(json.Length, 300)));

                    var doc = JsonDocument.Parse(json);

                    if (!doc.RootElement.TryGetProperty("machine_id", out var machineIdElement) || !doc.RootElement.TryGetProperty("image_base64", out var imageElement)) {
                        _logger.LogWarning("Missing 'machine_id' or 'image_base64' in event data.");
                        continue;
                    }

                    string machineId = machineIdElement.GetString()!;
                    string imageBase64 = imageElement.GetString()!;

                    // ML 推論リクエストの作成
                    var mlRequestBody = new {
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

                    // 画像を保存
                    string containerName = "images";
                    string dateFolder = DateTime.UtcNow.ToString("yyyyMMdd");
                    bool isPredicted = predictedClass == "class1";
                    string subFolder = isPredicted ? "on" : "off";
                    string folderPath = $"uploads/{dateFolder}/{subFolder}";
                    string fileName = isPredicted ? $"{machineId}_{DateTime.UtcNow:HHmmssfff}.jpg" : $"{machineId}_{DateTime.UtcNow:HHmmssfff}_{predictedClass}.jpg";
                    string fullBlobName = $"{folderPath}/{fileName}";
                    byte[] imageBytes = Convert.FromBase64String(imageBase64);
                    var blobContainerClient = new BlobContainerClient(_blobConnectionString, containerName);
                    await blobContainerClient.CreateIfNotExistsAsync();
                    await blobContainerClient.SetAccessPolicyAsync(Azure.Storage.Blobs.Models.PublicAccessType.None);

                    BlobClient blob = blobContainerClient.GetBlobClient(fullBlobName);

                    using (var stream = new MemoryStream(imageBytes)) {
                        await blob.UploadAsync(stream, overwrite: true);
                    }

                    bool isActivated = resultElement.GetBoolean();
                    string currentStatus = isActivated ? "1" : "0";
                    _logger.LogInformation("ML result for machine {machineId}: {result}", machineId, isActivated);

                    // 過去状態を確認
                    using var conn = new SqlConnection(_connectionString);
                    await conn.OpenAsync();
                    string? prevStatus = null;
                    using (var selectCmd = conn.CreateCommand()) {
                        selectCmd.CommandText = "SELECT activate FROM CurrentMachineStatus WHERE machine_id = @machine_id";
                        selectCmd.Parameters.AddWithValue("@machine_id", machineId);
                        var result = await selectCmd.ExecuteScalarAsync();
                        if (result != null) {
                            prevStatus = result.ToString();
                        }
                    }

                    // 状態が変化したときだけ処理
                    if (prevStatus != currentStatus) {
                        // OperationLogs に挿入
                        using (var logCmd = conn.CreateCommand()) {
                            logCmd.CommandText = "INSERT INTO OperationLogs (machine_id, activate) VALUES (@machine_id, @activate)";
                            logCmd.Parameters.AddWithValue("@machine_id", machineId);
                            logCmd.Parameters.AddWithValue("@activate", currentStatus);
                            await logCmd.ExecuteNonQueryAsync();
                        }

                        // CurrentMachineStatus を更新
                        using (var upsertCmd = conn.CreateCommand()) {
                            upsertCmd.CommandText = @"
                                MERGE CurrentMachineStatus AS target
                                USING (SELECT @machine_id AS machine_id, @activate AS activate) AS source
                                ON target.machine_id = source.machine_id
                                WHEN MATCHED THEN
                                    UPDATE SET activate = @activate, updated_at = GETUTCDATE()
                                WHEN NOT MATCHED THEN
                                    INSERT (machine_id, activate, updated_at) VALUES (@machine_id, @activate, GETUTCDATE());";
                            upsertCmd.Parameters.AddWithValue("@machine_id", machineId);
                            upsertCmd.Parameters.AddWithValue("@activate", currentStatus);
                            await upsertCmd.ExecuteNonQueryAsync();
                        }

                        _logger.LogInformation("Status changed. Inserted new record.");
                    } else {
                        _logger.LogInformation("No status change detected. Skipping DB insert.");
                    }
                } catch (JsonException jex) {
                    _logger.LogError(jex, "JSON parse failed");
                } catch (Exception ex) {
                    _logger.LogError(ex, "Error processing EventHub message.");
                }
            }
        }
    }
}
