using System;
using Microsoft.Data.SqlClient;
using System.Text.Json;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace Mizobata.Function
{
    public class DXFunction
    {
        private readonly ILogger<DXFunction> _logger;
        private readonly string? _connectionString;

        public DXFunction(ILogger<DXFunction> logger)
        {
            _logger = logger;
            _connectionString = Environment.GetEnvironmentVariable("ADO_NET_CONNECTION");
            if (string.IsNullOrEmpty(_connectionString)) {
                throw new InvalidOperationException("Missing ADO_NET_CONNECTION in environment variables.");
            }
        }

        [Function(nameof(DXFunction))]
        public void Run([EventHubTrigger(
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
                        machineIdElement.ValueKind != JsonValueKind.String ||
                        !doc.RootElement.TryGetProperty("activate", out var activateElement) ||
                        activateElement.ValueKind != JsonValueKind.String) {
                        _logger.LogWarning("Invalid or missing 'machine_id' or 'activate' in payload.");
                        return;
                    }

                    string machineId = machineIdElement.GetString()!;
                    string activate = activateElement.GetString()!;

                    _logger.LogInformation("Parsed machine_id: {machineId}, activate: {activate}", machineId, activate);

                    using var conn = new SqlConnection(_connectionString);
                    conn.Open();

                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = "INSERT INTO OperationLogs (machine_id, activate) VALUES (@machine_id, @activate)";
                    cmd.Parameters.AddWithValue("@machine_id", machineId);
                    cmd.Parameters.AddWithValue("@activate", activate);
                    cmd.ExecuteNonQuery();

                    _logger.LogInformation("Inserted into SQL DB successfully.");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to process and insert event.");
                }
            }
        }
    }
}
