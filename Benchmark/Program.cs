using BenchmarkDotNet.Running;
using System.Reflection;

namespace Benchmark
{
    class Program
    {
        static void Main(string[] args) => new BenchmarkSwitcher(typeof(Program).GetTypeInfo().Assembly).Run(args);
    }
}
