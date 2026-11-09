// The eyetable API as a Windows service: it runs scripts/run-public.ps1 (the API,
// the Cloudflare tunnel and the watchdog) and stops all of it with the service.
// Built and installed by scripts/install-service.ps1, which compiles this file
// with the C# compiler that comes with Windows.

using System;
using System.Diagnostics;
using System.IO;
using System.ServiceProcess;

public class EyetableService : ServiceBase
{
    public const string Name = "EyetableApi";
    Process runner;
    volatile bool stopping;

    public EyetableService()
    {
        ServiceName = Name;
        CanStop = true;
        CanShutdown = true;
        AutoLog = true;  // started / stopped entries in the Windows Application event log
    }

    static string ScriptsDir()
    {
        // this exe lives in scripts\service\
        return Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, ".."));
    }

    protected override void OnStart(string[] args)
    {
        string scripts = ScriptsDir();
        string powershell = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.System),
                                         @"WindowsPowerShell\v1.0\powershell.exe");
        var info = new ProcessStartInfo(powershell,
            "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File \"" + Path.Combine(scripts, "run-public.ps1") + "\"")
        {
            WorkingDirectory = Path.GetDirectoryName(scripts),
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        runner = Process.Start(info);
        runner.EnableRaisingEvents = true;
        runner.Exited += delegate
        {
            if (stopping) return;
            // The runner restarts the API and tunnel itself, so it ending is unexpected:
            // exit with an error and let the service's recovery settings start it again.
            EventLog.WriteEntry("run-public.ps1 exited (code " + runner.ExitCode + "); the service stops so Windows restarts it.",
                                EventLogEntryType.Error);
            Environment.Exit(1);
        };
    }

    protected override void OnStop() { StopRunner(); }

    protected override void OnShutdown() { StopRunner(); }

    void StopRunner()
    {
        stopping = true;
        if (runner == null || runner.HasExited) return;
        // The API and cloudflared are the runner's children: end the whole tree.
        using (var kill = Process.Start(new ProcessStartInfo("taskkill", "/T /F /PID " + runner.Id)
               { UseShellExecute = false, CreateNoWindow = true }))
        {
            kill.WaitForExit(15000);
        }
        runner.WaitForExit(10000);
    }

    public static void Main()
    {
        ServiceBase.Run(new EyetableService());
    }
}
