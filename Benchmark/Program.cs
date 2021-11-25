using BenchmarkDotNet.Running;
using System.Diagnostics;
using System.Reflection;
using System.Threading.Tasks;

namespace Benchmark
{
    class Program
    {
        //static void Main(string[] args) => new BenchmarkSwitcher(typeof(Program).GetTypeInfo().Assembly).Run(args);
        static async Task Main(string[] args)
        {
            var t = new ReadRowsPipelined();
            t.Queries = 10;
            await t.SetupRaw();

            var sw = Stopwatch.StartNew();
            while (true)
            {
                await t.ReadRawExtendedTest();
                var elapsed = sw.Elapsed;
                if (elapsed.TotalMilliseconds > 5)
                {
                    System.Console.WriteLine($"Elapsed: {elapsed.TotalMilliseconds}");
                }
                sw.Restart();
            }
        }
    }
}
