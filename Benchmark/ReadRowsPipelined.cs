using BenchmarkDotNet.Attributes;
using Npg.Core.Raw;
using Npgsql;
using System.Net;
using System.Threading.Tasks;

namespace Benchmark
{
    public class ReadRowsPipelined
    {
        const string DefaultConnectionString = "Server=127.0.0.1;User ID=postgres;Password=Master1234;Database=postgres;SSL Mode=Disable;Pooling=false";

        [Params(10)]
        public int Queries { get; set; }

        NpgsqlCommand Command { get; set; } = default!;

        string RawQuery = string.Empty;
        PgDB RawDB;

        [GlobalSetup(Target = nameof(ReadNpgsql))]
        public void SetupNpgsql()
        {
            var conn = new NpgsqlConnection(DefaultConnectionString);
            conn.Open();
            this.Command = new NpgsqlCommand($"SELECT 1", conn);
            //this.Command.Prepare();
        }

        [GlobalSetup(Targets = new[] { nameof(ReadRawSimple), nameof(ReadRawExtended) })]
        public async Task SetupRaw()
        {
            var endpoint = IPEndPoint.Parse("127.0.0.1:5432");
            this.RawDB = await PgDB.OpenAsync(endpoint, "postgres", "Master1234", "postgres");
            this.RawQuery = $"SELECT 1";
        }

        //[Benchmark]
        public async ValueTask ReadNpgsql()
        {
            for (var i = 0; i < Queries; i++)
            {
                await using var reader = await this.Command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {

                }
            }
        }

        //[Benchmark]
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
            var tasks = new ValueTask[Queries];
            var pipelineReadLock = await this.RawDB.EnterPipelineWriteLockAsync();
            tasks[0] = this.RawDB.ExecuteExtendedAsync(this.RawQuery);
            var readingTask = Task.Run(async () =>
            {
                if (pipelineReadLock is not null)
                {
                    await pipelineReadLock.Task;
                }

                for (var i = 0; i < tasks.Length; i++)
                {
                    await tasks[i];
                    await this.RawDB.EnsureSinglePacketAsync();
                    var responseCode = this.ReadPacket();

                    while (responseCode != BackendMessageCode.ReadyForQuery)
                    {
                        await this.RawDB.EnsureSinglePacketAsync();
                        responseCode = this.ReadPacket();
                    }
                }

                this.RawDB.CompletePipelineRead();
            });

            for (var i = 1; i < tasks.Length; i++)
            {
                tasks[i] = this.RawDB.ExecuteExtendedAsync(this.RawQuery);
            }

            if (pipelineReadLock is not null)
            {
                this.RawDB.ReleasePipelineWriteLock();
            }

            await readingTask;
        }

        public async ValueTask ReadRawExtendedTest()
        {
            for (var i = 0; i < Queries; i++)
            {
                await this.RawDB.ExecuteSyncAsync();
            }
            for (var i = 0; i < Queries; i++)
            {
                await this.RawDB.EnsureSinglePacketAsync();
                var responseCode = this.ReadPacket();

                while (responseCode != BackendMessageCode.ReadyForQuery)
                {
                    await this.RawDB.EnsureSinglePacketAsync();
                    responseCode = this.ReadPacket();
                }
            }
        }

        public BackendMessageCode ReadPacket()
        {
            var response = this.RawDB.ReadPacketAsSpan();
            return (BackendMessageCode)response[0];
        }
    }
}
