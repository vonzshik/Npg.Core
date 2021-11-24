using Pipelines.Sockets.Unofficial;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Npg.Core.Raw
{
    public sealed class PipePgDB : IDisposable
    {
        private readonly SocketConnection _connection;
        private readonly PipeWriteBuffer _writeBuffer;
        private readonly PipeReadBuffer _readBuffer;

        private int _packetLength = -1;
        private SequencePosition? _packetEnd;

        public PipePgDB(SocketConnection connection)
        {
            this._connection = connection;
            this._writeBuffer = new PipeWriteBuffer(this._connection.Output);
            this._readBuffer = new PipeReadBuffer(this._connection.Input);
        }

        public void Dispose()
        {
            this._connection.Dispose();
        }

        private static readonly UTF8Encoding UTF8Encoding = new(false, true);

        private static readonly UTF8Encoding RelaxedUTF8Encoding = new(false, false);

        public static async ValueTask<PipePgDB> OpenAsync(EndPoint endPoint, string username, string? password, string? database)
        {
            var connection = await SocketConnection.ConnectAsync(endPoint).ConfigureAwait(false);
            var db = new PipePgDB(connection);

            try
            {
                await db.WriteStartupAsync(username, database ?? username).ConfigureAwait(false);

                await db.Authenticate(username, password).ConfigureAwait(false);

                var msg = await db.ReadSinglePacketAsync().ConfigureAwait(false);
                var code = (BackendMessageCode) ReadByte(msg);
                while (code == BackendMessageCode.ParameterStatus)
                {
                    msg = await db.ReadSinglePacketAsync().ConfigureAwait(false);
                    code = (BackendMessageCode) ReadByte(msg);
                }
                if (code == BackendMessageCode.BackendKeyData)
                {
                    // skip
                    msg = await db.ReadSinglePacketAsync().ConfigureAwait(false);
                }
                ValidateResponseMessage(msg, BackendMessageCode.ReadyForQuery);

                return db;
            }
            catch
            {
                db.Dispose();
                throw;
            }
        }

        public ValueTask ExecuteSimpleAsync(string sql)
        {
            static void Write(PipePgDB db, string sql)
            {
                var bytes = db._writeBuffer.Buffer.Span.Slice(5);
                var length = UTF8Encoding.GetBytes(sql, bytes) + 1;
                bytes[length - 1] = 0;

                db._writeBuffer.WriteByte(FrontendMessageCode.Query);
                db._writeBuffer.WriteInt32(length + 4);
                db._writeBuffer.Position += length;
            }

            Write(this, sql);
            return this.FlushAsync();
        }

        public ValueTask ExecuteExtendedAsync(string sql)
        {
            static void Write(PipePgDB db, string sql)
            {
                var bytes = db._writeBuffer.Buffer.Span;

                var length = WriteParse(db, sql, bytes.Slice(5));
                bytes = bytes.Slice(length + 5);
                length = WriteBind(db, bytes.Slice(5));
                bytes = bytes.Slice(length + 5);
                length = WriteDescribe(db, bytes.Slice(5));
                bytes = bytes.Slice(length + 5);
                length = WriteExecute(db, bytes.Slice(5));
                bytes = bytes.Slice(length + 5);
                WriteSync(db, bytes.Slice(5));
            }

            static int WriteParse(PipePgDB db, string sql, Span<byte> bytes)
            {
                var fullLength = 0;

                // Statement name
                var length = Encoding.ASCII.GetBytes(string.Empty, bytes) + 1;
                fullLength += length;
                bytes[length - 1] = 0;
                bytes = bytes.Slice(length);

                // Query
                length = UTF8Encoding.GetBytes(sql, bytes) + 1;
                fullLength += length;
                bytes[length - 1] = 0;
                bytes = bytes.Slice(length);

                // Input parameters count
                BinaryPrimitives.WriteUInt16BigEndian(bytes, 0);
                fullLength += 2;

                db._writeBuffer.WriteByte(FrontendMessageCode.Parse);
                db._writeBuffer.WriteInt32(fullLength + 4);
                db._writeBuffer.Position += fullLength;

                return fullLength;
            }

            static int WriteBind(PipePgDB db, Span<byte> bytes)
            {
                var output = bytes;
                var fullLength = 0;

                // Portal
                bytes[0] = 0;
                bytes = bytes.Slice(1);
                var length = 1;
                fullLength = 1;

                // Statement name
                length = Encoding.ASCII.GetBytes(string.Empty, bytes) + 1;
                fullLength += length;
                bytes[length - 1] = 0;
                bytes = bytes.Slice(length);

                // Format code list length
                BinaryPrimitives.WriteInt16BigEndian(bytes, 0);
                fullLength += 2;
                bytes = bytes.Slice(2);

                // Input parameters count
                BinaryPrimitives.WriteUInt16BigEndian(bytes, 0);
                fullLength += 2;
                bytes = bytes.Slice(2);

                // All result types are known
                BinaryPrimitives.WriteInt16BigEndian(bytes, 1);
                fullLength += 2;
                bytes = bytes.Slice(2);

                BinaryPrimitives.WriteInt16BigEndian(bytes, 1);
                fullLength += 2;

                db._writeBuffer.WriteByte(FrontendMessageCode.Bind);
                db._writeBuffer.WriteInt32(fullLength + 4);
                db._writeBuffer.Position += fullLength;

                return fullLength;
            }

            static int WriteDescribe(PipePgDB db, Span<byte> bytes)
            {
                var output = bytes;
                var fullLength = 0;

                // Portal
                bytes[0] = (byte) StatementOrPortal.Portal;
                bytes = bytes.Slice(1);
                var length = 1;
                fullLength = 1;

                // Portal name
                length = Encoding.ASCII.GetBytes(string.Empty, bytes) + 1;
                fullLength += length;
                bytes[length - 1] = 0;

                db._writeBuffer.WriteByte(FrontendMessageCode.Describe);
                db._writeBuffer.WriteInt32(fullLength + 4);
                db._writeBuffer.Position += fullLength;

                return fullLength;
            }

            static int WriteExecute(PipePgDB db, Span<byte> bytes)
            {
                var output = bytes;
                var fullLength = 0;

                // Portal
                bytes[0] = 0;
                bytes = bytes.Slice(1);
                fullLength = 1;

                // Max rows
                BinaryPrimitives.WriteInt32BigEndian(bytes, 0);
                fullLength += 4;

                db._writeBuffer.WriteByte(FrontendMessageCode.Execute);
                db._writeBuffer.WriteInt32(fullLength + 4);
                db._writeBuffer.Position += fullLength;

                return fullLength;
            }

            static int WriteSync(PipePgDB db, Span<byte> bytes)
            {
                db._writeBuffer.WriteByte(FrontendMessageCode.Sync);
                db._writeBuffer.WriteInt32(4);
                return 0;
            }

            Write(this, sql);
            return this.FlushAsync();
        }

        public static string ParseSimpleQueryDataRowColumn(ReadOnlySequence<byte> column)
            => UTF8Encoding.GetString(column);

        private ValueTask FlushAsync() => this._writeBuffer.FlushAsync();

        private ValueTask WriteStartupAsync(string username, string database)
        {
            static void Write(PipePgDB db, string username, string database)
            {
                Span<byte> bytes = stackalloc byte[200];
                var length = GenerateStartup(bytes, username, database);
                db.WriteMessage(bytes.Slice(0, length));
            }

            Write(this, username, database);
            return this.FlushAsync();
        }

        private static int GenerateStartup(Span<byte> output, string userName, string database)
        {
            const int protocolVersion3 = 3 << 16; // 196608

            var bytesWritten = 0;

            BinaryPrimitives.WriteInt32BigEndian(output, protocolVersion3);
            bytesWritten += 4;
            output = output.Slice(4);

            var length = UTF8Encoding.GetBytes("user", output);
            output[length] = 0;
            output = output.Slice(length + 1);
            bytesWritten += length + 1;
            length = UTF8Encoding.GetBytes(userName, output);
            output[length] = 0;
            output = output.Slice(length + 1);
            bytesWritten += length + 1;

            length = UTF8Encoding.GetBytes("client_encoding", output);
            output[length] = 0;
            output = output.Slice(length + 1);
            bytesWritten += length + 1;
            length = UTF8Encoding.GetBytes("UTF8", output);
            output[length] = 0;
            output = output.Slice(length + 1);
            bytesWritten += length + 1;

            length = UTF8Encoding.GetBytes("database", output);
            output[length] = 0;
            output = output.Slice(length + 1);
            bytesWritten += length + 1;
            length = UTF8Encoding.GetBytes(database, output);
            output[length] = 0;
            output = output.Slice(length + 1);
            bytesWritten += length + 1;

            output[0] = 0;
            bytesWritten += 1;

            return bytesWritten;
        }

        private void WriteMessage(ReadOnlySpan<byte> message)
        {
            this._writeBuffer.WriteInt32(message.Length + 4);
            this._writeBuffer.WriteBytes(message);
        }

        private void WriteMessageWithCode(ReadOnlySpan<byte> message, byte messageCode)
        {
            this._writeBuffer.WriteByte(messageCode);
            this._writeBuffer.WriteInt32(message.Length + 4);
            this._writeBuffer.WriteBytes(message);
        }

        private async ValueTask Authenticate(string username, string? password)
        {
            var authenticateRequestMessage = await this.ReadSinglePacketAsync().ConfigureAwait(false);
            ValidateResponseMessage(authenticateRequestMessage, BackendMessageCode.AuthenticationRequest);

            var authenticationRequestType = (AuthenticationRequestType) ReadInt32BigEndian(authenticateRequestMessage.Slice(5));
            switch (authenticationRequestType)
            {
                case AuthenticationRequestType.Ok:
                    return;
                case AuthenticationRequestType.MD5Password:
                    await this.AuthenticateMD5(username, password, authenticateRequestMessage.Slice(9)).ConfigureAwait(false);
                    return;
                case AuthenticationRequestType.CleartextPassword:
                default:
                    throw new Exception();
            }
        }

        private static int ReadInt32BigEndian(ReadOnlySequence<byte> sequence)
        {
            var sq = new SequenceReader<byte>(sequence);
            var read = sq.TryReadBigEndian(out int value);
            Debug.Assert(read);
            return value;
        }

        private static byte ReadByte(ReadOnlySequence<byte> sequence)
        {
            var sq = new SequenceReader<byte>(sequence);
            var read = sq.TryRead(out var value);
            Debug.Assert(read);
            return value;
        }

        private async ValueTask AuthenticateMD5(string username, string? password, ReadOnlySequence<byte> salt)
        {
            if (password is null)
                throw new Exception();

            using var md5 = MD5.Create();

            // First phase
            var passwordBytes = UTF8Encoding.GetBytes(password);
            var usernameBytes = UTF8Encoding.GetBytes(username);
            var cryptBuf = new byte[passwordBytes.Length + usernameBytes.Length];
            passwordBytes.CopyTo(cryptBuf, 0);
            usernameBytes.CopyTo(cryptBuf, passwordBytes.Length);

            var sb = new StringBuilder();
            var hashResult = md5.ComputeHash(cryptBuf);
            foreach (var b in hashResult)
                sb.Append(b.ToString("x2"));

            var prehash = sb.ToString();

            var prehashbytes = UTF8Encoding.GetBytes(prehash);
            cryptBuf = new byte[prehashbytes.Length + 4];

            Array.Copy(salt.ToArray(), 0, cryptBuf, prehashbytes.Length, 4);

            // 2.
            prehashbytes.CopyTo(cryptBuf, 0);

            sb = new StringBuilder("md5");
            hashResult = md5.ComputeHash(cryptBuf);
            foreach (var b in hashResult)
                sb.Append(b.ToString("x2"));

            var resultString = sb.ToString();
            var result = new byte[UTF8Encoding.GetByteCount(resultString) + 1];
            UTF8Encoding.GetBytes(resultString, 0, resultString.Length, result, 0);
            result[result.Length - 1] = 0;

            this.WriteMessageWithCode(result, FrontendMessageCode.Password);
            await this.FlushAsync().ConfigureAwait(false);

            var resp = await this.ReadSinglePacketAsync().ConfigureAwait(false);
            ValidateResponseMessage(resp, BackendMessageCode.AuthenticationRequest);
        }

        public ValueTask<ReadOnlySequence<byte>> ReadSinglePacketAsync(CancellationToken cancellationToken = default)
        {
            if (_packetEnd.HasValue)
            {
                this._readBuffer.Consume(this._packetEnd.Value);
            }

            Debug.Assert(this._packetLength == -1);
            var packet = this._readBuffer.TryEnsureFast(5);
            if (packet.Length < 5)
            {
                return this.ReadSinglePacketAsyncSlow(cancellationToken);
            }

            var length = ReadInt32BigEndian(packet.Slice(1)) + 1;
            Debug.Assert(length < 8192);
            this._packetLength = length;
            if (packet.Length >= length)
            {
                return new ValueTask<ReadOnlySequence<byte>>(ReadPacket(packet));
            }

            this._readBuffer.Advance(packet);
            return this.ReadSinglePacketAsyncSlow(length, cancellationToken);
        }

        private async ValueTask<ReadOnlySequence<byte>> ReadSinglePacketAsyncSlow(CancellationToken cancellationToken = default)
        {
            var packet = await this._readBuffer.EnsureAsync(5).ConfigureAwait(false);
            var length = ReadInt32BigEndian(packet.Slice(1)) + 1;
            Debug.Assert(length < 8192);
            this._packetLength = length;
            if (packet.Length >= length)
            {
                return ReadPacket(packet);
            }

            this._readBuffer.Advance(packet);
            return await this.ReadSinglePacketAsyncSlow(length, cancellationToken).ConfigureAwait(false);
        }

        private async ValueTask<ReadOnlySequence<byte>> ReadSinglePacketAsyncSlow(int length, CancellationToken cancellationToken = default)
            => ReadPacket(await this._readBuffer.EnsureAsync(length).ConfigureAwait(false));

        private ReadOnlySequence<byte> ReadPacket(ReadOnlySequence<byte> packet)
        {
            var length = this._packetLength;
            Debug.Assert(packet.Length >= length);
            this._packetEnd = packet.GetPosition(length);
            this._packetLength = -1;
            return packet.Slice(0, length);
        }

        public static void ValidateResponseMessage(ReadOnlySequence<byte> response)
        {
            var responseType = (BackendMessageCode) ReadByte(response);
            if (responseType == BackendMessageCode.ErrorResponse)
            {
                // byte - code
                // int4 - length
                response = response.Slice(5);

                var message = ErrorOrNoticeMessage.Load(response);
                throw new Exception();
            }
        }

        public static void ValidateResponseMessage(ReadOnlySequence<byte> response, BackendMessageCode expectedResponseType)
        {
            var responseType = (BackendMessageCode) ReadByte(response);
            if (responseType == expectedResponseType)
            {
                return;
            }

            // byte - code
            // int4 - length
            response = response.Slice(5);

            if (responseType == BackendMessageCode.ErrorResponse)
            {
                var message = ErrorOrNoticeMessage.Load(response);
                throw new Exception();
            }

            throw new Exception();
        }

        private static string ReadString(SequenceReader<byte> bytes)
        {
            var read = bytes.TryReadTo(out ReadOnlySequence<byte> strBytes, 0);
            Debug.Assert(read);
            return RelaxedUTF8Encoding.GetString(strBytes);

            throw new Exception();
        }

        enum AuthenticationRequestType
        {
            Ok = 0,
            CleartextPassword = 3,
            MD5Password = 5,
        }

        static class FrontendMessageCode
        {
            internal const byte Describe = (byte) 'D';
            internal const byte Sync = (byte) 'S';
            internal const byte Execute = (byte) 'E';
            internal const byte Parse = (byte) 'P';
            internal const byte Bind = (byte) 'B';
            internal const byte Close = (byte) 'C';
            internal const byte Query = (byte) 'Q';
            internal const byte CopyData = (byte) 'd';
            internal const byte CopyDone = (byte) 'c';
            internal const byte CopyFail = (byte) 'f';
            internal const byte Terminate = (byte) 'X';
            internal const byte Password = (byte) 'p';
        }

        class ErrorOrNoticeMessage
        {
            internal string Severity { get; }
            internal string InvariantSeverity { get; }
            internal string SqlState { get; }
            internal string Message { get; }
            internal string? Detail { get; }
            internal string? Hint { get; }
            internal int Position { get; }
            internal int InternalPosition { get; }
            internal string? InternalQuery { get; }
            internal string? Where { get; }
            internal string? SchemaName { get; }
            internal string? TableName { get; }
            internal string? ColumnName { get; }
            internal string? DataTypeName { get; }
            internal string? ConstraintName { get; }
            internal string? File { get; }
            internal string? Line { get; }
            internal string? Routine { get; }

            internal static ErrorOrNoticeMessage Load(ReadOnlySequence<byte> bytes)
            {
                (string? severity, string? invariantSeverity, string? code, string? message, string? detail, string? hint) = (null, null, null, null, null, null);
                var (position, internalPosition) = (0, 0);
                (string? internalQuery, string? where) = (null, null);
                (string? schemaName, string? tableName, string? columnName, string? dataTypeName, string? constraintName) =
                    (null, null, null, null, null);
                (string? file, string? line, string? routine) = (null, null, null);

                var sq = new SequenceReader<byte>(bytes);

                while (true)
                {
                    var read = sq.TryRead(out var fieldCodeByte);
                    Debug.Assert(read);
                    var fieldCode = (ErrorFieldTypeCode) fieldCodeByte;

                    switch (fieldCode)
                    {
                        case ErrorFieldTypeCode.Done:
                            // Null terminator; error message fully consumed.
                            goto End;
                        case ErrorFieldTypeCode.Severity:
                            severity = ReadString(sq);
                            break;
                        case ErrorFieldTypeCode.InvariantSeverity:
                            invariantSeverity = ReadString(sq);
                            break;
                        case ErrorFieldTypeCode.Code:
                            code = ReadString(sq);
                            break;
                        case ErrorFieldTypeCode.Message:
                            message = ReadString(sq);
                            break;
                        case ErrorFieldTypeCode.Detail:
                            detail = ReadString(sq);
                            break;
                        case ErrorFieldTypeCode.Hint:
                            hint = ReadString(sq);
                            break;
                        case ErrorFieldTypeCode.Position:
                            var positionStr = ReadString(sq);
                            if (!int.TryParse(positionStr, out var tmpPosition))
                            {
                                continue;
                            }
                            position = tmpPosition;
                            break;
                        case ErrorFieldTypeCode.InternalPosition:
                            var internalPositionStr = ReadString(sq);
                            if (!int.TryParse(internalPositionStr, out var internalPositionTmp))
                            {
                                continue;
                            }
                            internalPosition = internalPositionTmp;
                            break;
                        case ErrorFieldTypeCode.InternalQuery:
                            internalQuery = ReadString(sq);
                            break;
                        case ErrorFieldTypeCode.Where:
                            where = ReadString(sq);
                            break;
                        case ErrorFieldTypeCode.File:
                            file = ReadString(sq);
                            break;
                        case ErrorFieldTypeCode.Line:
                            line = ReadString(sq);
                            break;
                        case ErrorFieldTypeCode.Routine:
                            routine = ReadString(sq);
                            break;
                        case ErrorFieldTypeCode.SchemaName:
                            schemaName = ReadString(sq);
                            break;
                        case ErrorFieldTypeCode.TableName:
                            tableName = ReadString(sq);
                            break;
                        case ErrorFieldTypeCode.ColumnName:
                            columnName = ReadString(sq);
                            break;
                        case ErrorFieldTypeCode.DataTypeName:
                            dataTypeName = ReadString(sq);
                            break;
                        case ErrorFieldTypeCode.ConstraintName:
                            constraintName = ReadString(sq);
                            break;
                        default:
                            // Unknown error field; consume and discard.
                            ReadString(sq);
                            break;
                    }
                }

            End:
                if (severity == null)
                    throw new Exception("Severity not received in server error message");
                if (code == null)
                    throw new Exception("Code not received in server error message");
                if (message == null)
                    throw new Exception("Message not received in server error message");

                return new ErrorOrNoticeMessage(
                    severity, invariantSeverity ?? severity, code, message,
                    detail, hint, position, internalPosition, internalQuery, where,
                    schemaName, tableName, columnName, dataTypeName, constraintName,
                    file, line, routine);

            }

            internal ErrorOrNoticeMessage(
                string severity, string invariantSeverity, string sqlState, string message,
                string? detail = null, string? hint = null, int position = 0, int internalPosition = 0, string? internalQuery = null, string? where = null,
                string? schemaName = null, string? tableName = null, string? columnName = null, string? dataTypeName = null, string? constraintName = null,
                string? file = null, string? line = null, string? routine = null)
            {
                this.Severity = severity;
                this.InvariantSeverity = invariantSeverity;
                this.SqlState = sqlState;
                this.Message = message;
                this.Detail = detail;
                this.Hint = hint;
                this.Position = position;
                this.InternalPosition = internalPosition;
                this.InternalQuery = internalQuery;
                this.Where = where;
                this.SchemaName = schemaName;
                this.TableName = tableName;
                this.ColumnName = columnName;
                this.DataTypeName = dataTypeName;
                this.ConstraintName = constraintName;
                this.File = file;
                this.Line = line;
                this.Routine = routine;
            }

            /// <summary>
            /// Error and notice message field codes
            /// </summary>
            internal enum ErrorFieldTypeCode : byte
            {
                Done = 0,
                Severity = (byte) 'S',
                InvariantSeverity = (byte) 'V',
                Code = (byte) 'C',
                Message = (byte) 'M',
                Detail = (byte) 'D',
                Hint = (byte) 'H',
                Position = (byte) 'P',
                InternalPosition = (byte) 'p',
                InternalQuery = (byte) 'q',
                Where = (byte) 'W',
                SchemaName = (byte) 's',
                TableName = (byte) 't',
                ColumnName = (byte) 'c',
                DataTypeName = (byte) 'd',
                ConstraintName = (byte) 'n',
                File = (byte) 'F',
                Line = (byte) 'L',
                Routine = (byte) 'R'
            }
        }

        enum StatementOrPortal : byte
        {
            Statement = (byte) 'S',
            Portal = (byte) 'P'
        }
    }
}
