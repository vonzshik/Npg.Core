using Pipelines.Sockets.Unofficial;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Npg.Core.Raw
{
    public sealed class PgDB : IDisposable
    {
        private readonly Socket _socket;
        private readonly Stream _stream;

        private readonly WriteBuffer _writeBuffer;
        private readonly ReadBuffer _readBuffer;

        private int _packetLength = -1;

        public PgDB(Socket socket)
        {
            this._socket = socket;
            this._stream = new NetworkStream(socket, true);
            this._writeBuffer = new WriteBuffer(this._stream);
            this._readBuffer = new ReadBuffer(this._stream);
        }

        public void Dispose()
        {
            this._stream.Dispose();
            this._readBuffer.Dispose();
            this._writeBuffer.Dispose();
        }

        private static readonly UTF8Encoding UTF8Encoding = new (false, true);

        private static readonly UTF8Encoding RelaxedUTF8Encoding = new (false, false);

        public static async ValueTask<PgDB> OpenAsync(EndPoint endPoint, string username, string? password, string? database)
        {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            await socket.ConnectAsync(endPoint).ConfigureAwait(false);
            var db = new PgDB(socket);

            try
            {
                await db.WriteStartupAsync(username, database ?? username).ConfigureAwait(false);

                await db.Authenticate(username, password).ConfigureAwait(false);

                await db.EnsureSinglePacketAsync().ConfigureAwait(false);
                var msg = db.ReadPacket();
                var code = (BackendMessageCode)msg.Span[0];
                while (code == BackendMessageCode.ParameterStatus)
                {
                    await db.EnsureSinglePacketAsync().ConfigureAwait(false);
                    msg = db.ReadPacket();
                    code = (BackendMessageCode)msg.Span[0];
                }
                if (code == BackendMessageCode.BackendKeyData)
                {
                    // skip
                    await db.EnsureSinglePacketAsync().ConfigureAwait(false);
                    msg = db.ReadPacket();
                }
                ValidateResponseMessage(msg.Span, BackendMessageCode.ReadyForQuery);

                return db;
            }
            catch
            {
                db.Dispose();
                throw;
            }
        }

        public async ValueTask ExecuteSimpleAsync(string sql)
        {
            static void Write(PgDB db, string sql)
            {
                var bytes = db._writeBuffer.Buffer.Span.Slice(5);
                var length = UTF8Encoding.GetBytes(sql, bytes) + 1;
                bytes[length - 1] = 0;

                db._writeBuffer.WriteByte(FrontendMessageCode.Query);
                db._writeBuffer.WriteInt32(length + 4);
                db._writeBuffer.Position += length;
            }

            Write(this, sql);
            await this.FlushAsync().ConfigureAwait(false);
        }

        public ValueTask ExecuteExtendedAsync(string sql)
        {
            static void Write(PgDB db, string sql)
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

            static int WriteParse(PgDB db, string sql, Span<byte> bytes)
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

            static int WriteBind(PgDB db, Span<byte> bytes)
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

            static int WriteDescribe(PgDB db, Span<byte> bytes)
            {
                var output = bytes;
                var fullLength = 0;

                // Portal
                bytes[0] = (byte)StatementOrPortal.Portal;
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

            static int WriteExecute(PgDB db, Span<byte> bytes)
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

            static int WriteSync(PgDB db, Span<byte> bytes)
            {
                db._writeBuffer.WriteByte(FrontendMessageCode.Sync);
                db._writeBuffer.WriteInt32(4);
                return 0;
            }

            Write(this, sql);
            return this.FlushAsync();
        }

        public static string ParseSimpleQueryDataRowColumn(ReadOnlyMemory<byte> column)
            => UTF8Encoding.GetString(column.Span);

        public static string ParseSimpleQueryDataRowColumn(ReadOnlySpan<byte> column)
            => UTF8Encoding.GetString(column);

        private ValueTask FlushAsync() => this._writeBuffer.FlushAsync();

        private ValueTask WriteStartupAsync(string username, string database)
        {
            static void Write(PgDB db, string username, string database)
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
            await this.EnsureSinglePacketAsync().ConfigureAwait(false);
            var authenticateRequestMessage = this.ReadPacket();
            ValidateResponseMessage(authenticateRequestMessage.Span, BackendMessageCode.AuthenticationRequest);

            var authenticationRequestType = (AuthenticationRequestType)BinaryPrimitives.ReadInt32BigEndian(authenticateRequestMessage.Span.Slice(5, 4));
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

        private async ValueTask AuthenticateMD5(string username, string? password, ReadOnlyMemory<byte> salt)
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

            await this.EnsureSinglePacketAsync().ConfigureAwait(false);
            var resp = this.ReadPacket();
            ValidateResponseMessage(resp.Span, BackendMessageCode.AuthenticationRequest);
        }

        public ValueTask EnsureSinglePacketAsync(CancellationToken cancellationToken = default)
        {
            Debug.Assert(this._packetLength == -1);
            var packet = this._readBuffer.TryEnsureFast(5);
            if (packet.Length < 5)
            {
                return this.EnsureSinglePacketAsyncSlow(cancellationToken);
            }

            var length = BinaryPrimitives.ReadInt32BigEndian(packet.Span.Slice(1, 4)) + 1;
            Debug.Assert(length < 8192);
            if (packet.Length >= length)
            {
                this._packetLength = length;
                return new ValueTask();
            }

            return this.EnsureSinglePacketAsyncSlow(cancellationToken);
        }

        private async ValueTask EnsureSinglePacketAsyncSlow(CancellationToken cancellationToken = default)
        {
            var packet = await this._readBuffer.EnsureAsync(5).ConfigureAwait(false);
            var length = BinaryPrimitives.ReadInt32BigEndian(packet.Span.Slice(1, 4)) + 1;
            Debug.Assert(length < 8192);
            this._packetLength = length;
            if (packet.Length >= length)
            {
                return;
            }

            await this._readBuffer.EnsureAsync(length).ConfigureAwait(false);
            return;
        }

        public ReadOnlyMemory<byte> ReadPacket()
        {
            var length = this._packetLength;
            var packet = this._readBuffer.TryEnsureFast(length);
            Debug.Assert(packet.Length >= length);
            this._readBuffer.MovePosition(this._packetLength);
            this._packetLength = -1;
            return packet.Slice(0, length);
        }

        public ReadOnlySpan<byte> ReadPacketAsSpan()
        {
            var length = this._packetLength;
            var packet = this._readBuffer.TryEnsureFast(length).Span;
            Debug.Assert(packet.Length >= this._packetLength);
            this._readBuffer.MovePosition(length);
            this._packetLength = -1;
            return packet.Slice(0, length);
        }

        public static void ValidateResponseMessage(ReadOnlyMemory<byte> response)
            => ValidateResponseMessage(response.Span);

        public static void ValidateResponseMessage(ReadOnlySpan<byte> response)
        {
            var responseType = (BackendMessageCode)response[0];
            if (responseType == BackendMessageCode.ErrorResponse)
            {
                // byte - code
                // int4 - length
                response = response.Slice(5);

                var message = ErrorOrNoticeMessage.Load(response);
                throw new Exception();
            }
        }

        public static void ValidateResponseMessage(ReadOnlyMemory<byte> response, BackendMessageCode expectedResponseType)
            => ValidateResponseMessage(response.Span, expectedResponseType);

        public static void ValidateResponseMessage(ReadOnlySpan<byte> response, BackendMessageCode expectedResponseType)
        {
            var responseType = (BackendMessageCode)response[0];
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

        private static string ReadString(ReadOnlySpan<byte> bytes, out int length)
        {
            for (var i = 0; i < bytes.Length; i++)
            {
                if (bytes[i] == 0)
                {
                    length = i + 1;
                    return RelaxedUTF8Encoding.GetString(bytes.Slice(0, i));
                }
            }

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
            internal const byte Describe =  (byte)'D';
            internal const byte Sync =      (byte)'S';
            internal const byte Execute =   (byte)'E';
            internal const byte Parse =     (byte)'P';
            internal const byte Bind =      (byte)'B';
            internal const byte Close =     (byte)'C';
            internal const byte Query =     (byte)'Q';
            internal const byte CopyData =  (byte)'d';
            internal const byte CopyDone =  (byte)'c';
            internal const byte CopyFail =  (byte)'f';
            internal const byte Terminate = (byte)'X';
            internal const byte Password =  (byte)'p';
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

            internal static ErrorOrNoticeMessage Load(ReadOnlySpan<byte> bytes)
            {
                (string? severity, string? invariantSeverity, string? code, string? message, string? detail, string? hint) = (null, null, null, null, null, null);
                var (position, internalPosition) = (0, 0);
                (string? internalQuery, string? where) = (null, null);
                (string? schemaName, string? tableName, string? columnName, string? dataTypeName, string? constraintName) =
                    (null, null, null, null, null);
                (string? file, string? line, string? routine) = (null, null, null);

                while (true)
                {
                    var fieldCode = (ErrorFieldTypeCode)bytes[0];
                    bytes = bytes.Slice(1);

                    int length;
                    switch (fieldCode)
                    {
                        case ErrorFieldTypeCode.Done:
                            // Null terminator; error message fully consumed.
                            goto End;
                        case ErrorFieldTypeCode.Severity:
                            severity = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            break;
                        case ErrorFieldTypeCode.InvariantSeverity:
                            invariantSeverity = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            break;
                        case ErrorFieldTypeCode.Code:
                            code = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            break;
                        case ErrorFieldTypeCode.Message:
                            message = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            break;
                        case ErrorFieldTypeCode.Detail:
                            detail = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            break;
                        case ErrorFieldTypeCode.Hint:
                            hint = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            break;
                        case ErrorFieldTypeCode.Position:
                            var positionStr = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            if (!int.TryParse(positionStr, out var tmpPosition))
                            {
                                continue;
                            }
                            position = tmpPosition;
                            break;
                        case ErrorFieldTypeCode.InternalPosition:
                            var internalPositionStr = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            if (!int.TryParse(internalPositionStr, out var internalPositionTmp))
                            {
                                continue;
                            }
                            internalPosition = internalPositionTmp;
                            break;
                        case ErrorFieldTypeCode.InternalQuery:
                            internalQuery = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            break;
                        case ErrorFieldTypeCode.Where:
                            where = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            break;
                        case ErrorFieldTypeCode.File:
                            file = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            break;
                        case ErrorFieldTypeCode.Line:
                            line = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            break;
                        case ErrorFieldTypeCode.Routine:
                            routine = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            break;
                        case ErrorFieldTypeCode.SchemaName:
                            schemaName = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            break;
                        case ErrorFieldTypeCode.TableName:
                            tableName = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            break;
                        case ErrorFieldTypeCode.ColumnName:
                            columnName = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            break;
                        case ErrorFieldTypeCode.DataTypeName:
                            dataTypeName = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            break;
                        case ErrorFieldTypeCode.ConstraintName:
                            constraintName = ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
                            break;
                        default:
                            // Unknown error field; consume and discard.
                            ReadString(bytes, out length);
                            bytes = bytes.Slice(length);
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
                Severity            = (byte)'S',
                InvariantSeverity   = (byte)'V',
                Code                = (byte)'C',
                Message             = (byte)'M',
                Detail              = (byte)'D',
                Hint                = (byte)'H',
                Position            = (byte)'P',
                InternalPosition    = (byte)'p',
                InternalQuery       = (byte)'q',
                Where               = (byte)'W',
                SchemaName          = (byte)'s',
                TableName           = (byte)'t',
                ColumnName          = (byte)'c',
                DataTypeName        = (byte)'d',
                ConstraintName      = (byte)'n',
                File                = (byte)'F',
                Line                = (byte)'L',
                Routine             = (byte)'R'
            }
        }

        enum StatementOrPortal : byte
        {
            Statement   = (byte)'S',
            Portal      = (byte)'P'
        }
    }
    
    public enum BackendMessageCode : byte
    {
        AuthenticationRequest   = (byte)'R',
        BackendKeyData          = (byte)'K',
        BindComplete            = (byte)'2',
        CloseComplete           = (byte)'3',
        CommandComplete         = (byte)'C',
        CopyData                = (byte)'d',
        CopyDone                = (byte)'c',
        CopyBothResponse        = (byte)'W',
        CopyInResponse          = (byte)'G',
        CopyOutResponse         = (byte)'H',
        DataRow                 = (byte)'D',
        EmptyQueryResponse      = (byte)'I',
        ErrorResponse           = (byte)'E',
        FunctionCall            = (byte)'F',
        FunctionCallResponse    = (byte)'V',
        NoData                  = (byte)'n',
        NoticeResponse          = (byte)'N',
        NotificationResponse    = (byte)'A',
        ParameterDescription    = (byte)'t',
        ParameterStatus         = (byte)'S',
        ParseComplete           = (byte)'1',
        PasswordPacket          = (byte)' ',
        PortalSuspended         = (byte)'s',
        ReadyForQuery           = (byte)'Z',
        RowDescription          = (byte)'T',
    }
}
