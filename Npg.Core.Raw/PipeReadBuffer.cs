using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace Npg.Core.Raw
{
    public sealed class PipeReadBuffer
    {
        private readonly PipeReader _input;

        public PipeReadBuffer(PipeReader input)
        {
            this._input = input;
        }

        private ReadOnlySequence<byte>? _buffer;
        private long _offset;

        public ReadOnlySequence<byte> TryEnsureFast(int bytes)
        {
            if (_buffer.HasValue)
            {
                if (_buffer.Value.Length - _offset >= bytes)
                {
                    return _buffer.Value.Slice(_offset);
                }

                this.Advance(_buffer.Value);
                _buffer = null;
            }

            if (_input.TryRead(out var result))
            {
                var buffer = result.Buffer;
                if (buffer.Length - _offset >= bytes)
                {
                    _buffer = buffer;
                    return buffer.Slice(_offset);
                }

                this.Advance(buffer);
            }

            return ReadOnlySequence<byte>.Empty;
        }

        public async ValueTask<ReadOnlySequence<byte>> EnsureAsync(int bytes)
        {
            if (this._buffer.HasValue)
            {
                this.Advance(this._buffer.Value);
            }

            while (true)
            {
                var result = await _input.ReadAsync().ConfigureAwait(false);
                var buffer = result.Buffer;
                if (buffer.Length - this._offset >= bytes)
                {
                    _buffer = buffer;
                    return buffer.Slice(_offset);
                }

                this.Advance(buffer);
            }
        }

        public void Advance(ReadOnlySequence<byte> sequence) => this._input.AdvanceTo(sequence.Start, sequence.End);

        public void Consume(int length)
        {
            if (this._buffer.HasValue)
            {
                _offset += length;
                if (_offset > 8192)
                {
                    this._input.AdvanceTo(this._buffer.Value.Slice(_offset).Start);
                    _offset = 0;
                    _buffer = null;
                }
            }
        }
    }
}
