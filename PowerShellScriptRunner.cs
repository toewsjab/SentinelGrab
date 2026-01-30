using System.Diagnostics;
using System.Text;

public sealed record ScriptResult(int ExitCode, string Stdout, string Stderr, TimeSpan Duration);

public sealed class PowerShellScriptRunner
{
    public async Task<ScriptResult> RunAsync(string scriptPath, IReadOnlyDictionary<string, string> parameters)
    {
        if (!File.Exists(scriptPath))
        {
            throw new FileNotFoundException("Script not found", scriptPath);
        }

        var args = new StringBuilder();
        args.Append("-NoProfile -ExecutionPolicy Bypass -File ");
        args.Append(EscapeArgument(scriptPath));

        foreach (var kvp in parameters)
        {
            args.Append(' ');
            args.Append('-');
            args.Append(kvp.Key);
            args.Append(' ');
            args.Append(EscapeArgument(kvp.Value));
        }

        var psi = new ProcessStartInfo
        {
            FileName = "powershell.exe",
            Arguments = args.ToString(),
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using var process = new Process { StartInfo = psi };
        var start = DateTime.UtcNow;
        process.Start();

        var stdoutTask = process.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();

        await Task.WhenAll(stdoutTask, stderrTask, process.WaitForExitAsync());

        var duration = DateTime.UtcNow - start;
        return new ScriptResult(process.ExitCode, stdoutTask.Result, stderrTask.Result, duration);
    }

    private static string EscapeArgument(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return "\"\"";
        }

        var escaped = value.Replace("\"", "`\"");
        return $"\"{escaped}\"";
    }
}
