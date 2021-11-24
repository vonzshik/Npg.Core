using System;
using System.Buffers;
using System.IO;
using System.Threading.Tasks;

namespace Npg.Core.Raw
{
    public sealed class ReadBuffer : IDisposable
    {
        private readonly Stream _stream;

        private readonly Memory<byte> _buffer;
        private readonly IMemoryOwner<byte> _bufferOwner;

        private int _position;
        private int _read;

        public ReadBuffer(Stream stream)
        {
            this._stream = stream;
            this._bufferOwner = MemoryPool<byte>.Shared.Rent(8192);
            this._buffer = this._bufferOwner.Memory;
        }

        public ReadOnlyMemory<byte> TryEnsureFast(int bytes)
        {
            if (this._read - this._position >= bytes)
            {
                return this._buffer.Slice(this._position, this._read - this._position);
            }

            return Array.Empty<byte>();
        }

        public async ValueTask<ReadOnlyMemory<byte>> EnsureAsync(int bytes)
        {
            if (this._read - this._position >= bytes)
            {
                return this._buffer.Slice(this._position, this._read - this._position);
            }

            if (this._position == this._read)
            {
                this._position = 0;
                this._read = 0;
            }

            if (this._buffer.Length - this._position < bytes)
            {
                var nextReadPosition = this._read - this._position;
                this._buffer.Slice(this._position, nextReadPosition).CopyTo(this._buffer);
                this._read = nextReadPosition;
                this._position = 0;
            }

            var read = this._read;
            var count = this._buffer.Length - read;

            while (read < bytes)
            {
                read += await this._stream.ReadAsync(this._buffer.Slice(read, count)).ConfigureAwait(false);
                count -= read;
            }

            this._read = read;
            return this._buffer.Slice(this._position, this._read - this._position);
        }

        public void MovePosition(int count) => this._position += count;

        public void Dispose()
        {
            this._bufferOwner.Dispose();
        }
    }
}
