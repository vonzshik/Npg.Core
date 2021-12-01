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

        public ReadOnlySequence<byte> TryEnsureFast(int bytes)
        {
            if (_input.TryRead(out var result))
            {
                var buffer = result.Buffer;
                if (buffer.Length >= bytes)
                {
                    return buffer;
                }

                this.Advance(buffer);
            }

            return ReadOnlySequence<byte>.Empty;
        }

        public async ValueTask<ReadOnlySequence<byte>> EnsureAsync(int bytes)
        {
            while (true)
            {
                var result = await _input.ReadAsync().ConfigureAwait(false);
                var buffer = result.Buffer;
                if (buffer.Length >= bytes)
                {
                    return buffer.Slice(0, bytes);
                }

                this.Advance(buffer);
            }
        }

        public void Advance(ReadOnlySequence<byte> sequence) => this._input.AdvanceTo(sequence.Start, sequence.End);

        public void Consume(SequencePosition position) => this._input.AdvanceTo(position);
    }
}
