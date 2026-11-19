using System.Data.Common;
using System.Net.Sockets;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Text;
using AutoMapper;
using Microsoft.Extensions.Hosting;
using Miningcore.Configuration;
using Miningcore.Extensions;
using Miningcore.Messaging;
using Miningcore.Notifications.Messages;
using Miningcore.Persistence;
using Miningcore.Persistence.Model;
using Miningcore.Persistence.Repositories;
using Newtonsoft.Json;
using NLog;
using Polly;
using Polly.CircuitBreaker;
using Contract = Miningcore.Contracts.Contract;
using Share = Miningcore.Blockchain.Share;
using static Miningcore.Util.ActionUtils;

namespace Miningcore.Mining;

/// <summary>
/// Asynchronously persist shares produced by all pools for processing by coin-specific payment processor(s)
/// </summary>
public class ShareRecorder : BackgroundService
{
    public ShareRecorder(IConnectionFactory cf,
        IMapper mapper,
        JsonSerializerSettings jsonSerializerSettings,
        IShareRepository shareRepo,
        IBlockRepository blockRepo,
        ClusterConfig clusterConfig,
        IMessageBus messageBus)
    {
        Contract.RequiresNonNull(cf);
        Contract.RequiresNonNull(mapper);
        Contract.RequiresNonNull(shareRepo);
        Contract.RequiresNonNull(blockRepo);
        Contract.RequiresNonNull(jsonSerializerSettings);
        Contract.RequiresNonNull(messageBus);

        this.cf = cf;
        this.mapper = mapper;
        this.jsonSerializerSettings = jsonSerializerSettings;
        this.messageBus = messageBus;
        this.clusterConfig = clusterConfig;

        this.shareRepo = shareRepo;
        this.blockRepo = blockRepo;

        pools = clusterConfig.Pools.ToDictionary(x => x.Id, x => x);

        BuildFaultHandlingPolicy();
        ConfigureRecovery();
    }

    private static readonly ILogger logger = LogManager.GetCurrentClassLogger();
    private readonly IShareRepository shareRepo;
    private readonly IBlockRepository blockRepo;
    private readonly IConnectionFactory cf;
    private readonly JsonSerializerSettings jsonSerializerSettings;
    private readonly IMessageBus messageBus;
    private readonly ClusterConfig clusterConfig;
    private readonly Dictionary<string, PoolConfig> pools;
    private readonly IMapper mapper;

    private IAsyncPolicy faultPolicy;
    private bool hasLoggedPolicyFallbackFailure;
    private string recoveryFilename;
    private const int RetryCount = 3;
    private const string PolicyContextKeyShares = "share";
    private bool notifiedAdminOnPolicyFallback = false;
    private readonly SemaphoreSlim fileLock = new(1, 1);
    private DateTime lastFallback = DateTime.MinValue;

    private async Task PersistSharesAsync(IList<Share> shares)
    {
        var context = new Dictionary<string, object> { { PolicyContextKeyShares, shares } };

        await faultPolicy.ExecuteAsync(ctx => PersistSharesCoreAsync((IList<Share>) ctx[PolicyContextKeyShares]), context);
    }

    private async Task PersistSharesCoreAsync(IList<Share> shares)
    {
        // A miner can put anything in its name or agent string. One NUL character makes PostgreSQL
        // refuse the whole batch, which would drop every other miner's shares in it too.
        foreach(var share in shares)
        {
            share.Miner = Clean(share.Miner);
            share.Worker = Clean(share.Worker);
            share.UserAgent = Clean(share.UserAgent);
            share.IpAddress = Clean(share.IpAddress);
            share.Source = Clean(share.Source);
        }

        await cf.RunTx(async (con, tx) =>
        {
            // Insert shares
            var mapped = shares.Select(mapper.Map<Persistence.Model.Share>).ToArray();
            await shareRepo.BatchInsertAsync(con, tx, mapped, CancellationToken.None);

            // Insert blocks
            foreach(var share in shares)
            {
                if(!share.IsBlockCandidate)
                    continue;

                var blockEntity = mapper.Map<Block>(share);
                blockEntity.Status = BlockStatus.Pending;
                await blockRepo.InsertAsync(con, tx, blockEntity);

                // share recovery runs without the pools started, so their coin templates are not loaded
                if(pools.TryGetValue(share.PoolId, out var poolConfig) && poolConfig.Template == null)
                    logger.Info(() => $"Recorded block {blockEntity.BlockHeight} for pool {share.PoolId}");
                else if(poolConfig != null)
                    messageBus.NotifyBlockFound(share.PoolId, blockEntity, poolConfig.Template);
                else
                    logger.Warn(()=> $"Block found for unknown pool {share.PoolId}");
            }
        });
    }

    private static string Clean(string value)
    {
        if(string.IsNullOrEmpty(value) || !value.Any(char.IsControl))
            return value;

        return new string(value.Where(c => !char.IsControl(c)).ToArray());
    }

    private static void OnPolicyRetry(Exception ex, TimeSpan timeSpan, int retry, object context)
    {
        logger.Warn(() => $"Retry {retry} in {timeSpan} due to {ex.Source}: {ex.GetType().Name} ({ex.Message})");
    }

    private Task OnPolicyFallbackAsync(Exception ex, Context context)
    {
        logger.Warn(() => $"Fallback due to {ex.Source}: {ex.GetType().Name} ({ex.Message})");
        return Task.CompletedTask;
    }

    private async Task OnExecutePolicyFallbackAsync(Context context, CancellationToken ct)
    {
        var shares = (IList<Share>) context[PolicyContextKeyShares];

        // One share the database refuses must not cost the others in its batch
        var unsaved = await PersistOneByOneAsync(shares);

        if(unsaved.Count == 0)
            return;

        if(unsaved.Count < shares.Count)
            logger.Warn(() => $"{unsaved.Count} of {shares.Count} shares could not be saved and go to the recovery file; the others were saved one by one");

        await AppendToFileAsync(recoveryFilename, unsaved, true);
        lastFallback = DateTime.UtcNow;

        NotifyAdminOnPolicyFallback();
    }

    /// <summary>
    /// Save shares one at a time. Returns the ones that could not be saved.
    /// Gives up early when nothing gets through at all, which means the database itself is unreachable.
    /// </summary>
    private async Task<List<Share>> PersistOneByOneAsync(IList<Share> shares)
    {
        var unsaved = new List<Share>();

        if(shares.Count <= 1)
        {
            unsaved.AddRange(shares);
            return unsaved;
        }

        var saved = 0;

        for(var i = 0; i < shares.Count; i++)
        {
            if(saved == 0 && unsaved.Count >= 3)
            {
                unsaved.AddRange(shares.Skip(i));
                break;
            }

            try
            {
                await PersistSharesCoreAsync(new List<Share> { shares[i] });
                saved++;
            }

            catch(Exception ex)
            {
                if(unsaved.Count == 0)
                    logger.Warn(() => $"Share of {shares[i].Miner} for pool {shares[i].PoolId} could not be saved: {ex.Message}");

                unsaved.Add(shares[i]);
            }
        }

        return unsaved;
    }

    private async Task AppendToFileAsync(string filename, IList<Share> shares, bool withHeader)
    {
        try
        {
            await fileLock.WaitAsync();

            try
            {
                await using var stream = new FileStream(filename, FileMode.Append, FileAccess.Write);
                await using var writer = new StreamWriter(stream, new UTF8Encoding(false));

                if(withHeader && stream.Length == 0)
                    WriteRecoveryFileheader(writer);

                foreach(var share in shares)
                    await writer.WriteLineAsync(JsonConvert.SerializeObject(share, jsonSerializerSettings));
            }

            finally
            {
                fileLock.Release();
            }
        }

        catch(Exception ex)
        {
            if(!hasLoggedPolicyFallbackFailure)
            {
                logger.Fatal(ex, $"Could not write to {filename}. Share(s) will be lost!");
                hasLoggedPolicyFallbackFailure = true;
            }
        }
    }

    private static void WriteRecoveryFileheader(TextWriter writer)
    {
        writer.WriteLine("# The existence of this file means shares could not be committed to the database.");
        writer.WriteLine("# You should stop the pool cluster and run the following command:");
        writer.WriteLine("# miningcore -c <path-to-config> -rs <path-to-this-file>\n");
    }

    public async Task RecoverSharesAsync(string filename)
    {
        logger.Info(() => $"Recovering shares using {filename} ...");

        try
        {
            var successCount = 0;
            var failCount = 0;
            const int bufferSize = 100;

            await using(var stream = new FileStream(filename, FileMode.Open, FileAccess.Read))
            {
                using(var reader = new StreamReader(stream, new UTF8Encoding(false)))
                {
                    var shares = new List<Share>();
                    var lastProgressUpdate = DateTime.UtcNow;

                    while(!reader.EndOfStream)
                    {
                        var line = await reader.ReadLineAsync();

                        if(string.IsNullOrEmpty(line))
                            continue;

                        // skip blank lines
                        line = line.Trim();

                        if(line.Length == 0)
                            continue;

                        // skip comments
                        if(line.StartsWith("#"))
                            continue;

                        // parse
                        try
                        {
                            var share = JsonConvert.DeserializeObject<Share>(line, jsonSerializerSettings);
                            shares.Add(share);
                        }

                        catch(JsonException ex)
                        {
                            logger.Error(ex, () => $"Unable to parse share record: {line}");
                            failCount++;
                        }

                        // import
                        try
                        {
                            if(shares.Count == bufferSize)
                            {
                                await PersistSharesCoreAsync(shares);

                                successCount += shares.Count;
                                shares.Clear();
                            }
                        }

                        catch(Exception ex)
                        {
                            logger.Error(ex, () => $"Unable to import a batch of {shares.Count} shares, retrying them one by one");

                            var unsaved = await PersistOneByOneAsync(shares);
                            successCount += shares.Count - unsaved.Count;
                            failCount += unsaved.Count;

                            if(unsaved.Count > 0)
                                await AppendToFileAsync(filename + ".failed", unsaved, false);

                            // start the next batch clean, or no later share would ever be imported
                            shares.Clear();
                        }

                        // progress
                        var now = DateTime.UtcNow;

                        if(now - lastProgressUpdate > TimeSpan.FromSeconds(10))
                        {
                            logger.Info($"{successCount} shares imported");
                            lastProgressUpdate = now;
                        }
                    }

                    // import remaining shares
                    try
                    {
                        if(shares.Count > 0)
                        {
                            await PersistSharesCoreAsync(shares);

                            successCount += shares.Count;
                        }
                    }

                    catch(Exception ex)
                    {
                        logger.Error(ex, () => $"Unable to import the last {shares.Count} shares, retrying them one by one");

                        var unsaved = await PersistOneByOneAsync(shares);
                        successCount += shares.Count - unsaved.Count;
                        failCount += unsaved.Count;

                        if(unsaved.Count > 0)
                            await AppendToFileAsync(filename + ".failed", unsaved, false);
                    }
                }
            }

            if(failCount == 0)
                logger.Info(() => $"Successfully imported {successCount} shares");
            else
                logger.Warn(() => $"Imported {successCount} shares; {failCount} could not be imported and are kept in {filename}.failed");
        }

        catch(FileNotFoundException)
        {
            logger.Error(() => $"Recovery file {filename} was not found");
        }
    }

    private void NotifyAdminOnPolicyFallback()
    {
        if(!notifiedAdminOnPolicyFallback)
        {
            notifiedAdminOnPolicyFallback = true;

            logger.Error(() => $"Shares could not be saved to the database and are being kept in {recoveryFilename}. They are replayed automatically; if this message returns, find out why the database refuses them.");

            messageBus.SendMessage(new AdminNotification("Share Recorder Policy Fallback",
                $"The Share Recorder's Policy Fallback has been engaged. Check share recovery file {recoveryFilename}."));
        }
    }

    private void ConfigureRecovery()
    {
        recoveryFilename = !string.IsNullOrEmpty(clusterConfig.ShareRecoveryFile)
            ? clusterConfig.ShareRecoveryFile
            : "recovered-shares.txt";

        // never depend on the directory the pool happened to be started from
        if(!Path.IsPathRooted(recoveryFilename))
            recoveryFilename = Path.Combine(AppContext.BaseDirectory, recoveryFilename);
    }

    private void BuildFaultHandlingPolicy()
    {
        // retry with increasing delay (1s, 2s, 4s etc)
        var retry = Policy
            .Handle<DbException>()
            .Or<SocketException>()
            .Or<TimeoutException>()
            .WaitAndRetryAsync(RetryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                OnPolicyRetry);

        // after retries failed several times, break the circuit and fall through to
        // fallback action for one minute, not attempting further retries during that period
        var breaker = Policy
            .Handle<DbException>()
            .Or<SocketException>()
            .Or<TimeoutException>()
            .CircuitBreakerAsync(2, TimeSpan.FromMinutes(1));

        var fallback = Policy
            .Handle<DbException>()
            .Or<SocketException>()
            .Or<TimeoutException>()
            .FallbackAsync(OnExecutePolicyFallbackAsync, OnPolicyFallbackAsync);

        var fallbackOnBrokenCircuit = Policy
            .Handle<BrokenCircuitException>()
            .FallbackAsync(OnExecutePolicyFallbackAsync, (ex, context) => Task.CompletedTask);

        faultPolicy = Policy.WrapAsync(
            fallbackOnBrokenCircuit,
            Policy.WrapAsync(fallback, breaker, retry));
    }

    /// <summary>
    /// Put shares that were set aside back into the database without anyone having to run a command:
    /// shortly after start, and again whenever the file reappears and has been quiet for a while.
    /// </summary>
    private async Task ReplayRecoveryFileAsync(CancellationToken ct)
    {
        // "-rs <file>" is the manual way of doing the same thing. Stay out of its way.
        if(Environment.GetCommandLineArgs().Any(x => x is "-rs" or "--recover-shares"))
            return;

        try
        {
            await Task.Delay(TimeSpan.FromMinutes(1), ct);

            while(!ct.IsCancellationRequested)
            {
                try
                {
                    var file = new FileInfo(recoveryFilename);

                    if(file.Exists && file.Length > 0 && DateTime.UtcNow - lastFallback > TimeSpan.FromMinutes(5))
                    {
                        var replay = $"{recoveryFilename}.replay-{DateTime.UtcNow:yyyyMMddHHmmss}";

                        await fileLock.WaitAsync(ct);

                        try
                        {
                            File.Move(recoveryFilename, replay);
                        }

                        finally
                        {
                            fileLock.Release();
                        }

                        notifiedAdminOnPolicyFallback = false;

                        logger.Info(() => $"Replaying shares that were set aside ({replay})");
                        await RecoverSharesAsync(replay);

                        File.Move(replay, replay.Replace(".replay-", ".done-"));
                    }
                }

                catch(Exception ex) when(ex is not OperationCanceledException)
                {
                    logger.Error(ex, () => "Could not replay the share recovery file");
                }

                await Task.Delay(TimeSpan.FromMinutes(5), ct);
            }
        }

        catch(OperationCanceledException)
        {
            // shutting down
        }
    }

    protected override Task ExecuteAsync(CancellationToken ct)
    {
        logger.Info(() => "Online");

        _ = Task.Run(() => ReplayRecoveryFileAsync(ct), ct);

        return messageBus.Listen<Share>()
            .ObserveOn(TaskPoolScheduler.Default)
            .Where(x => x != null)
            .Select(x => x)
            .Buffer(TimeSpan.FromSeconds(5), 250)
            .Where(shares => shares.Any())
            .Select(shares => Observable.FromAsync(() =>
                Guard(() =>
                        PersistSharesAsync(shares),
                    ex => logger.Error(ex))))
            .Concat()
            .ToTask(ct)
            .ContinueWith(task =>
            {
                if(task.IsFaulted)
                    logger.Fatal(() => $"Terminated due to error {task.Exception?.InnerException ?? task.Exception}");
                else
                    logger.Info(() => "Offline");
            }, ct);
    }
}
