using System;
using System.Buffers;
using System.Diagnostics;
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

        private ReadOnlySequence<byte> _buffer = ReadOnlySequence<byte>.Empty;
        private long _offset;

        public ReadOnlySequence<byte> TryEnsureFast(int bytes)
        {
            if (!_buffer.IsEmpty)
            {
                if (_buffer.Length - _offset >= bytes)
                {
                    return _buffer.Slice(_offset);
                }

                this.Advance(_buffer.Slice(_offset));
                _offset = 0;
                _buffer = ReadOnlySequence<byte>.Empty;
            }

            Debug.Assert(_offset == 0);
            if (_input.TryRead(out var result))
            {
                var buffer = result.Buffer;
                if (buffer.Length >= bytes)
                {
                    _buffer = buffer;
                    return buffer;
                }

                this.Advance(buffer);
            }

            return ReadOnlySequence<byte>.Empty;
        }

        public async ValueTask<ReadOnlySequence<byte>> EnsureAsync(int bytes)
        {
            Debug.Assert(_offset == 0);
            while (true)
            {
                var result = await _input.ReadAsync().ConfigureAwait(false);
                var buffer = result.Buffer;
                if (buffer.Length >= bytes)
                {
                    _buffer = buffer;
                    return buffer;
                }

                this.Advance(buffer);
            }
        }

        private void Advance(ReadOnlySequence<byte> sequence) => this._input.AdvanceTo(sequence.Start, sequence.End);

        public void Consume(int length)
        {
            _offset += length;
        }
    }
}
