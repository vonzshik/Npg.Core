using BenchmarkDotNet.Running;
using System.Reflection;
using System.Threading.Tasks;

namespace Benchmark
{
    class Program
    {
        static void Main(string[] args) => new BenchmarkSwitcher(typeof(Program).GetTypeInfo().Assembly).Run(args);
    }
}
