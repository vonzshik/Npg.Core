using BenchmarkDotNet.Attributes;
using Npg.Core.Raw;
using Npgsql;
using System;
using System.Buffers;
using System.Net;
using System.Threading.Tasks;

namespace Benchmark
{
    public class ReadRows
    {
        const string DefaultConnectionString = "Server=127.0.0.1;User ID=postgres;Password=Master1234;Database=postgres;SSL Mode=Disable;Pooling=false";

        [Params(1, 10, 100, 1000)]
        public int NumRows { get; set; }

        NpgsqlCommand Command { get; set; } = default!;

        string RawQuery = string.Empty;
        PgDB RawDB;
        PipePgDB PipeRawDB;

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

        [GlobalSetup(Targets = new[] { nameof(ReadPipeRawSimple), nameof(ReadPipeRawExtended), nameof(ReadPipeRawExtendedMultiple) })]
        public async Task SetupPipeRaw()
        {
            var endpoint = IPEndPoint.Parse("127.0.0.1:5432");
            this.PipeRawDB = await PipePgDB.OpenAsync(endpoint, "postgres", "Master1234", "postgres");
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

        [Benchmark]
        public async ValueTask ReadPipeRawSimple()
        {
            await this.PipeRawDB.ExecuteSimpleAsync(this.RawQuery);

            var response = await this.PipeRawDB.ReadSinglePacketAsync();
            var responseCode = ReadPacket(response);

            while (responseCode != BackendMessageCode.ReadyForQuery)
            {
                response = await this.PipeRawDB.ReadSinglePacketAsync();
                responseCode = ReadPacket(response);
            }
        }

        [Benchmark]
        public async ValueTask ReadPipeRawExtended()
        {
            await this.PipeRawDB.ExecuteExtendedAsync(this.RawQuery);

            var response = await this.PipeRawDB.ReadSinglePacketAsync();
            var responseCode = ReadPacket(response);

            while (responseCode != BackendMessageCode.ReadyForQuery)
            {
                response = await this.PipeRawDB.ReadSinglePacketAsync();
                responseCode = ReadPacket(response);
            }
        }

        public static BackendMessageCode ReadPacket(ReadOnlySequence<byte> response)
        {
            var sq = new SequenceReader<byte>(response);
            sq.TryRead(out var read);
            return (BackendMessageCode)read;
        }

        [Benchmark]
        public async ValueTask ReadPipeRawExtendedMultiple()
        {
            await this.PipeRawDB.ExecuteExtendedAsync(this.RawQuery);

            var completed = false;
            while (!completed)
            {
                var response = await this.PipeRawDB.ReadMultiplePacketsAsync();
                completed = ReadPackets(response, out var read);
                this.PipeRawDB.Consume(read);
            }
        }

        public static bool ReadPackets(ReadOnlySequence<byte> response, out long read)
        {
            var reader = new SequenceReader<byte>(response);

            while (true)
            {
                if (!reader.TryRead(out var codeByte))
                {
                    read = reader.Consumed;
                    return false;
                }

                var code = (BackendMessageCode)codeByte;
                if (!reader.TryReadBigEndian(out int length))
                {
                    read = reader.Consumed - 1;
                    return false;
                }

                length -= 4;
                if (reader.Remaining < length)
                {
                    read = reader.Consumed - 5;
                    return false;
                }
                
                if (code == BackendMessageCode.ReadyForQuery)
                {
                    read = reader.Consumed + length;
                    return true;
                }

                reader.Advance(length);
            }
        }
    }
}
