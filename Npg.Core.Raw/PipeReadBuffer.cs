using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace Npg.Core.Raw
{
    public sealed class PipeReadBuffer
    {
        private readonly PipeReader _input;

        public byte[] PacketBuffer { get; } = new byte[4096];

        private ReadOnlySequence<byte> _buffer = ReadOnlySequence<byte>.Empty;

        public PipeReadBuffer(PipeReader input)
        {
            this._input = input;
        }

        public ReadOnlySequence<byte> TryEnsureFast(int bytes)
        {
            if (!_buffer.IsEmpty)
            {
                if (_buffer.Length >= bytes)
                {
                    return _buffer;
                }

                this.Advance(_buffer);
                _buffer = ReadOnlySequence<byte>.Empty;
            }

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
            if (!_buffer.IsEmpty)
            {
                this.Advance(_buffer);
            }

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

        public void Consume(long length)
        {
            this._input.AdvanceTo(_buffer.GetPosition(length));
            _buffer = ReadOnlySequence<byte>.Empty;
        }
    }
}
