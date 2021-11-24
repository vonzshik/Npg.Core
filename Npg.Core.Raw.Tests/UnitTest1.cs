using NUnit.Framework;
using System.Buffers.Binary;
using System.Net;
using System.Threading.Tasks;

namespace Npg.Core.Raw.Tests
{
    public class Tests
    {
        const string EndPoint = "127.0.0.1:5432";
        const string UserName = "postgres";
        const string Password = "Master1234";
        const string Database = "postgres";

        [Test]
        public async Task SimpleQueryEmptyQuery()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PgDB.OpenAsync(endpoint, UserName, Password, Database);
            await db.ExecuteSimpleAsync(string.Empty);

            await db.EnsureSinglePacketAsync();
            var response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.EmptyQueryResponse);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);
        }

        [Test]
        public async Task ExtendedQueryEmptyQuery()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PgDB.OpenAsync(endpoint, UserName, Password, Database);
            await db.ExecuteExtendedAsync(string.Empty);

            await db.EnsureSinglePacketAsync();
            var response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.ParseComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.BindComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.NoData);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.EmptyQueryResponse);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);
        }

        [Test]
        public async Task SimpleQueryNoRows()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PgDB.OpenAsync(endpoint, UserName, Password, Database);
            await db.ExecuteSimpleAsync("SELECT 1 WHERE 1 = 0");

            await db.EnsureSinglePacketAsync();
            var response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.CommandComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);
        }

        [Test]
        public async Task ExtendedQueryNoRows()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PgDB.OpenAsync(endpoint, UserName, Password, Database);
            await db.ExecuteExtendedAsync("SELECT 1 WHERE 1 = 0");

            await db.EnsureSinglePacketAsync();
            var response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.ParseComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.BindComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.CommandComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);
        }

        [Test]
        public async Task SimpleQuerySingleRow()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PgDB.OpenAsync(endpoint, UserName, Password, Database);
            await db.ExecuteSimpleAsync("SELECT 1");

            await db.EnsureSinglePacketAsync();
            var response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.DataRow);

            var columnCount = BinaryPrimitives.ReadInt16BigEndian(response.Slice(5, 2).Span);
            Assert.AreEqual(1, columnCount);

            var columnLength = BinaryPrimitives.ReadInt32BigEndian(response.Slice(7, 4).Span);
            Assert.AreEqual(1, columnLength);

            var columnValueChar = (char)response.Span[11];
            Assert.AreEqual('1', columnValueChar);
            var columnValueString = PgDB.ParseSimpleQueryDataRowColumn(response.Slice(11));
            Assert.AreEqual("1", columnValueString);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.CommandComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);
        }

        [Test]
        public async Task ExtendedQuerySingleRow()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PgDB.OpenAsync(endpoint, UserName, Password, Database);
            await db.ExecuteExtendedAsync("SELECT 1");

            await db.EnsureSinglePacketAsync();
            var response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.ParseComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.BindComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.DataRow);

            var columnCount = BinaryPrimitives.ReadInt16BigEndian(response.Slice(5, 2).Span);
            Assert.AreEqual(1, columnCount);

            var columnLength = BinaryPrimitives.ReadInt32BigEndian(response.Slice(7, 4).Span);
            Assert.AreEqual(4, columnLength);

            var columnValue = BinaryPrimitives.ReadInt32BigEndian(response.Slice(11).Span);
            Assert.AreEqual(1, columnValue);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.CommandComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);
        }

        [Test]
        public async Task ExtendedQueryFortunesSingleRow()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PgDB.OpenAsync(endpoint, UserName, Password, "Fortunes");
            await db.ExecuteExtendedAsync("SELECT * FROM \"fortune\"");

            await db.EnsureSinglePacketAsync();
            var response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.ParseComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.BindComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.DataRow);

            var columnCount = BinaryPrimitives.ReadInt16BigEndian(response.Slice(5, 2).Span);
            Assert.AreEqual(2, columnCount);

            var columnLength = BinaryPrimitives.ReadInt32BigEndian(response.Slice(7, 4).Span);
            Assert.AreEqual(4, columnLength);

            var columnValue = BinaryPrimitives.ReadInt32BigEndian(response.Slice(11, columnLength).Span);
            Assert.AreEqual(1, columnValue);

            var secondColumnLength = BinaryPrimitives.ReadInt32BigEndian(response.Slice(15, 4).Span);
            Assert.AreEqual(34, secondColumnLength);

            var secondColumnValue = PgDB.ParseSimpleQueryDataRowColumn(response.Slice(19, secondColumnLength));
        }

        [Test]
        public async Task SimpleQueryMultipleRows()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PgDB.OpenAsync(endpoint, UserName, Password, Database);

            var rows = 42;

            await db.ExecuteSimpleAsync($"SELECT * FROM generate_series(1, {rows})");

            await db.EnsureSinglePacketAsync();
            var response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            for (var i = 1; i <= rows; i++)
            {
                var iStr = i.ToString();

                await db.EnsureSinglePacketAsync();
                response = db.ReadPacket();
                PgDB.ValidateResponseMessage(response, BackendMessageCode.DataRow);

                var columnCount = BinaryPrimitives.ReadInt16BigEndian(response.Slice(5, 2).Span);
                Assert.AreEqual(1, columnCount);

                var columnLength = BinaryPrimitives.ReadInt32BigEndian(response.Slice(7, 4).Span);
                Assert.AreEqual(iStr.Length, columnLength);

                var columnValueString = PgDB.ParseSimpleQueryDataRowColumn(response.Slice(11));
                Assert.AreEqual(iStr, columnValueString);
            }

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.CommandComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);
        }

        [Test]
        public async Task ExtendedQueryMultipleRows()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PgDB.OpenAsync(endpoint, UserName, Password, Database);

            var rows = 42;

            await db.ExecuteExtendedAsync($"SELECT * FROM generate_series(1, {rows})");

            await db.EnsureSinglePacketAsync();
            var response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.ParseComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.BindComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            for (var i = 1; i <= rows; i++)
            {
                await db.EnsureSinglePacketAsync();
                response = db.ReadPacket();
                PgDB.ValidateResponseMessage(response, BackendMessageCode.DataRow);

                var columnCount = BinaryPrimitives.ReadInt16BigEndian(response.Slice(5, 2).Span);
                Assert.AreEqual(1, columnCount);

                var columnLength = BinaryPrimitives.ReadInt32BigEndian(response.Slice(7, 4).Span);
                Assert.AreEqual(4, columnLength);

                var columnValue = BinaryPrimitives.ReadInt32BigEndian(response.Slice(11).Span);
                Assert.AreEqual(i, columnValue);
            }

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.CommandComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);
        }

        [Test]
        public async Task SimpleQueryMultipleResultSets()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PgDB.OpenAsync(endpoint, UserName, Password, Database);

            var rows = 42;

            await db.ExecuteSimpleAsync($"SELECT * FROM generate_series(1, {rows});SELECT * FROM generate_series(1, {rows})");

            await db.EnsureSinglePacketAsync();
            var response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            for (var i = 1; i <= rows; i++)
            {
                var iStr = i.ToString();

                await db.EnsureSinglePacketAsync();
                response = db.ReadPacket();
                PgDB.ValidateResponseMessage(response, BackendMessageCode.DataRow);

                var columnCount = BinaryPrimitives.ReadInt16BigEndian(response.Slice(5, 2).Span);
                Assert.AreEqual(1, columnCount);

                var columnLength = BinaryPrimitives.ReadInt32BigEndian(response.Slice(7, 4).Span);
                Assert.AreEqual(iStr.Length, columnLength);

                var columnValueString = PgDB.ParseSimpleQueryDataRowColumn(response.Slice(11));
                Assert.AreEqual(iStr, columnValueString);
            }

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.CommandComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.RowDescription);

            for (var i = 1; i <= rows; i++)
            {
                var iStr = i.ToString();

                await db.EnsureSinglePacketAsync();
                response = db.ReadPacket();
                PgDB.ValidateResponseMessage(response, BackendMessageCode.DataRow);

                var columnCount = BinaryPrimitives.ReadInt16BigEndian(response.Slice(5, 2).Span);
                Assert.AreEqual(1, columnCount);

                var columnLength = BinaryPrimitives.ReadInt32BigEndian(response.Slice(7, 4).Span);
                Assert.AreEqual(iStr.Length, columnLength);

                var columnValueString = PgDB.ParseSimpleQueryDataRowColumn(response.Slice(11));
                Assert.AreEqual(iStr, columnValueString);
            }

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.CommandComplete);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);
        }

        [Test]
        public async Task ExtendedQueryMultipleResultSets()
        {
            var endpoint = IPEndPoint.Parse(EndPoint);
            using var db = await PgDB.OpenAsync(endpoint, UserName, Password, Database);

            await db.ExecuteExtendedAsync($"SELECT 1; SELECT 2;");

            await db.EnsureSinglePacketAsync();
            var response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.ErrorResponse);

            await db.EnsureSinglePacketAsync();
            response = db.ReadPacket();
            PgDB.ValidateResponseMessage(response, BackendMessageCode.ReadyForQuery);
        }
    }
}