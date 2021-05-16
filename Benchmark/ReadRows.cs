using BenchmarkDotNet.Attributes;
using Npg.Core.Raw;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Benchmark
{
    public class ReadRows
    {
        const string DefaultConnectionString = "Server=localhost;User ID=postgres;Password=Master1234;Database=postgres";

        [Params(1, 10, 100, 1000)]
        public int NumRows { get; set; }

        NpgsqlCommand Command { get; set; } = default!;

        string RawQuery = string.Empty;
        PgDB RawDB;

        [GlobalSetup(Target = nameof(ReadNpgsql))]
        public void SetupNpgsql()
        {
            var conn = new NpgsqlConnection(DefaultConnectionString);
            conn.Open();
            this.Command = new NpgsqlCommand($"SELECT generate_series(1, {this.NumRows})", conn);
            //this.Command.Prepare();
        }

        [GlobalSetup(Targets = new[] { nameof(ReadRawSimple), nameof(ReadRawExtended) })]
        public async Task SetupRaw()
        {
            var endpoint = IPEndPoint.Parse("127.0.0.1:5432");
            this.RawDB = await PgDB.OpenAsync(endpoint, "postgres", "Master1234", "postgres");
            this.RawQuery = $"SELECT generate_series(1, {this.NumRows})";
        }

        [Benchmark]
        public async ValueTask ReadNpgsql()
        {
            await using var reader = await this.Command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {

            }
        }

        [Benchmark]
        public async ValueTask ReadRawSimple()
        {
            await this.RawDB.ExecuteSimpleAsync(this.RawQuery);

            await this.RawDB.EnsureSinglePacketAsync();
            var response = this.RawDB.ReadPacket();
            var responseCode = (BackendMessageCode)response.Span[0];

            while (responseCode != BackendMessageCode.ReadyForQuery)
            {
                await this.RawDB.EnsureSinglePacketAsync();
                response = this.RawDB.ReadPacket();
                responseCode = (BackendMessageCode)response.Span[0];
            }
        }

        [Benchmark]
        public async ValueTask ReadRawExtended()
        {
            await this.RawDB.ExecuteExtendedAsync(this.RawQuery);

            await this.RawDB.EnsureSinglePacketAsync();
            var responseCode = this.ReadPacket();

            while (responseCode != BackendMessageCode.ReadyForQuery)
            {
                await this.RawDB.EnsureSinglePacketAsync();
                responseCode = this.ReadPacket();
            }
        }

        public BackendMessageCode ReadPacket()
        {
            var response = this.RawDB.ReadPacketAsSpan();
            return (BackendMessageCode)response[0];
        }
    }
}
