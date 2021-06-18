using System;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace Npg.Core.Raw
{
    public class PipeWriteBuffer
    {
        private readonly PipeWriter _output;

        private Memory<byte> _buffer;

        private int _position;

        public PipeWriteBuffer(PipeWriter output)
        {
            this._output = output;
            this._buffer = this._output.GetMemory(8192);
        }

        public Memory<byte> Buffer => this._buffer.Slice(this._position);

        public int Position
        {
            get => this._position;
            set => this._position = value;
        }

        public async ValueTask FlushAsync()
        {
            if (this._position == 0)
                return;

            this._output.Advance(this._position);
            await this._output.FlushAsync().ConfigureAwait(false);
            this._buffer = this._output.GetMemory(8192);
            this._position = 0;
        }

        public void WriteBytes(ReadOnlySpan<byte> bytes)
        {
            bytes.CopyTo(this._buffer.Slice(this._position).Span);
            this._position += bytes.Length;
        }

        public void WriteInt32(int value)
        {
            BinaryPrimitives.WriteInt32BigEndian(this._buffer.Slice(this._position).Span, value);
            this._position += 4;
        }

        public void WriteByte(byte value)
        {
            this._buffer.Span[this._position] = value;
            this._position++;
        }
    }
}
