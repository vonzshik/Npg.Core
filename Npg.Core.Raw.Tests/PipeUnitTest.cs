using NUnit.Framework;
using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;

namespace Npg.Core.Raw.Tests
{
    public class PipeTests
    {
        const string EndPoint = "127.0.0.1:5432";
        const string UserName = "postgres";
        const string Password = "Master1234";
        const string Database = "postgres";

        [Test]
        public async Task SimpleQueryEmptyQuery()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PipePgDB.OpenAsync(endpoint, UserName, Password, Database);
            await db.ExecuteSimpleAsync(string.Empty);

            var response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.EmptyQueryResponse);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);
        }

        [Test]
        public async Task ExtendedQueryEmptyQuery()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PipePgDB.OpenAsync(endpoint, UserName, Password, Database);
            await db.ExecuteExtendedAsync(string.Empty);

            var response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.ParseComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.BindComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.NoData);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.EmptyQueryResponse);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);
        }

        [Test]
        public async Task SimpleQueryNoRows()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PipePgDB.OpenAsync(endpoint, UserName, Password, Database);
            await db.ExecuteSimpleAsync("SELECT 1 WHERE 1 = 0");

            var response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.CommandComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);
        }

        [Test]
        public async Task ExtendedQueryNoRows()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PipePgDB.OpenAsync(endpoint, UserName, Password, Database);
            await db.ExecuteExtendedAsync("SELECT 1 WHERE 1 = 0");

            var response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.ParseComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.BindComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.CommandComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);
        }

        [Test]
        public async Task SimpleQuerySingleRow()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PipePgDB.OpenAsync(endpoint, UserName, Password, Database);
            await db.ExecuteSimpleAsync("SELECT 1");

            var response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.DataRow);

            ReadPacket(response);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.CommandComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);

            static void ReadPacket(ReadOnlySequence<byte> packet)
            {
                var sq = new SequenceReader<byte>(packet.Slice(5));

                var columnCount = ReadInt16BigEndian(ref sq);
                Assert.AreEqual(1, columnCount);

                var columnLength = ReadInt32BigEndian(ref sq);
                Assert.AreEqual(1, columnLength);

                var columnValueString = PipePgDB.ParseSimpleQueryDataRowColumn(sq.UnreadSequence);
                Assert.AreEqual("1", columnValueString);
            }
        }

        [Test]
        public async Task ExtendedQuerySingleRow()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PipePgDB.OpenAsync(endpoint, UserName, Password, Database);
            await db.ExecuteExtendedAsync("SELECT 1");

            var response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.ParseComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.BindComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.DataRow);

            ReadPacket(response);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.CommandComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);

            static void ReadPacket(ReadOnlySequence<byte> packet)
            {
                var sq = new SequenceReader<byte>(packet.Slice(5));

                var columnCount = ReadInt16BigEndian(ref sq);
                Assert.AreEqual(1, columnCount);

                var columnLength = ReadInt32BigEndian(ref sq);
                Assert.AreEqual(4, columnLength);

                var columnValue = ReadInt32BigEndian(ref sq);
                Assert.AreEqual(1, columnValue);
            }
        }

        [Test]
        public async Task ExtendedQueryFortunesSingleRow()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PipePgDB.OpenAsync(endpoint, UserName, Password, "Fortunes");
            await db.ExecuteExtendedAsync("SELECT * FROM \"fortune\"");

            var response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.ParseComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.BindComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.DataRow);

            ReadPacket(response);

            static void ReadPacket(ReadOnlySequence<byte> packet)
            {
                var sq = new SequenceReader<byte>(packet.Slice(5));

                var columnCount = ReadInt16BigEndian(ref sq);
                Assert.AreEqual(2, columnCount);

                var columnLength = ReadInt32BigEndian(ref sq);
                Assert.AreEqual(4, columnLength);

                var columnValue = ReadInt32BigEndian(ref sq);
                Assert.AreEqual(1, columnValue);

                var secondColumnLength = ReadInt32BigEndian(ref sq);
                Assert.AreEqual(34, secondColumnLength);

                var secondColumnValue = PipePgDB.ParseSimpleQueryDataRowColumn(sq.UnreadSequence);
            }
        }

        [Test]
        public async Task SimpleQueryMultipleRows()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PipePgDB.OpenAsync(endpoint, UserName, Password, Database);

            var rows = 42;

            await db.ExecuteSimpleAsync($"SELECT * FROM generate_series(1, {rows})");

            var response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            for (var i = 1; i <= rows; i++)
            {
                var iStr = i.ToString();

                response = await db.ReadSinglePacketAsync();
                PipePgDB.ValidateResponseMessage(response, BackendMessageCode.DataRow);

                ReadPacket(response, iStr);
            }

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.CommandComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);

            static void ReadPacket(ReadOnlySequence<byte> packet, string iStr)
            {
                var sq = new SequenceReader<byte>(packet.Slice(5));

                var columnCount = ReadInt16BigEndian(ref sq);
                Assert.AreEqual(1, columnCount);

                var columnLength = ReadInt32BigEndian(ref sq);
                Assert.AreEqual(iStr.Length, columnLength);

                var columnValueString = PipePgDB.ParseSimpleQueryDataRowColumn(sq.UnreadSequence);
                Assert.AreEqual(iStr, columnValueString);
            }
        }

        [Test]
        public async Task ExtendedQueryMultipleRows()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PipePgDB.OpenAsync(endpoint, UserName, Password, Database);

            var rows = 42;

            await db.ExecuteExtendedAsync($"SELECT * FROM generate_series(1, {rows})");

            var response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.ParseComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.BindComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            for (var i = 1; i <= rows; i++)
            {
                response = await db.ReadSinglePacketAsync();
                PipePgDB.ValidateResponseMessage(response, BackendMessageCode.DataRow);

                ReadPacket(response, i);
            }

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.CommandComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);

            static void ReadPacket(ReadOnlySequence<byte> packet, int i)
            {
                var sq = new SequenceReader<byte>(packet.Slice(5));

                var columnCount = ReadInt16BigEndian(ref sq);
                Assert.AreEqual(1, columnCount);

                var columnLength = ReadInt32BigEndian(ref sq);
                Assert.AreEqual(4, columnLength);

                var columnValue = ReadInt32BigEndian(ref sq);
                Assert.AreEqual(i, columnValue);
            }
        }

        [Test]
        public async Task SimpleQueryMultipleResultSets()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PipePgDB.OpenAsync(endpoint, UserName, Password, Database);

            var rows = 42;

            await db.ExecuteSimpleAsync($"SELECT * FROM generate_series(1, {rows});SELECT * FROM generate_series(1, {rows})");

            var response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            for (var i = 1; i <= rows; i++)
            {
                var iStr = i.ToString();

                response = await db.ReadSinglePacketAsync();
                PipePgDB.ValidateResponseMessage(response, BackendMessageCode.DataRow);

                ReadPacket(response, iStr);
            }

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.CommandComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            for (var i = 1; i <= rows; i++)
            {
                var iStr = i.ToString();

                response = await db.ReadSinglePacketAsync();
                PipePgDB.ValidateResponseMessage(response, BackendMessageCode.DataRow);

                ReadPacket(response, iStr);
            }

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.CommandComplete);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);

            static void ReadPacket(ReadOnlySequence<byte> packet, string iStr)
            {
                var sq = new SequenceReader<byte>(packet.Slice(5));

                var columnCount = ReadInt16BigEndian(ref sq);
                Assert.AreEqual(1, columnCount);

                var columnLength = ReadInt32BigEndian(ref sq);
                Assert.AreEqual(iStr.Length, columnLength);

                var columnValueString = PipePgDB.ParseSimpleQueryDataRowColumn(sq.UnreadSequence);
                Assert.AreEqual(iStr, columnValueString);
            }
        }

        [Test]
        public async Task ExtendedQueryMultipleResultSets()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PipePgDB.OpenAsync(endpoint, UserName, Password, Database);

            await db.ExecuteExtendedAsync($"SELECT 1; SELECT 2;");

            var response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.ErrorResponse);

            response = await db.ReadSinglePacketAsync();
            PipePgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);
        }

        static short ReadInt16BigEndian(ref SequenceReader<byte> sq)
        {
            var read = sq.TryReadBigEndian(out short value);
            Debug.Assert(read);
            return value;
        }

        static int ReadInt32BigEndian(ref SequenceReader<byte> sq)
        {
            var read = sq.TryReadBigEndian(out int value);
            Debug.Assert(read);
            return value;
        }
    }
}