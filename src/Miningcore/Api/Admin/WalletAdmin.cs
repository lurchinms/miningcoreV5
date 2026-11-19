using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Text.RegularExpressions;
using System.Net.Http;
using Dapper;
using Miningcore.Blockchain.Kaspa;
using Miningcore.Blockchain.Kaspa.Configuration;
using Miningcore.Blockchain.Pearl.Configuration;
using Miningcore.Blockchain.Quantus;
using Miningcore.Blockchain.Quantus.Configuration;
using Miningcore.Blockchain.Zano;
using Miningcore.Blockchain.Zano.DaemonRequests;
using Miningcore.Blockchain.Zano.DaemonResponses;
using Miningcore.Configuration;
using Miningcore.Extensions;
using Miningcore.Messaging;
using Miningcore.Notifications.Messages;
using Miningcore.Persistence;
using Miningcore.Rpc;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using kaspaWalletd = Miningcore.Blockchain.Kaspa.KaspaWalletd;

namespace Miningcore.Api.Admin;

/// <summary>
/// Pool wallet balances and manual sends for the admin panel. One wallet per coin: every pool of a
/// coin pays out of the same wallet, so balances owed to miners are summed across those pools.
/// </summary>
public class WalletAdmin
{
    public WalletAdmin(ClusterConfig clusterConfig, JsonSerializerSettings serializerSettings, IMessageBus messageBus, IConnectionFactory cf)
    {
        this.clusterConfig = clusterConfig;
        this.serializerSettings = serializerSettings;
        this.messageBus = messageBus;
        this.cf = cf;
    }

    private readonly ClusterConfig clusterConfig;
    private readonly JsonSerializerSettings serializerSettings;
    private readonly IMessageBus messageBus;
    private readonly IConnectionFactory cf;
    private static readonly ILogger logger = LogManager.GetLogger("WalletAdmin");
    private static readonly SemaphoreSlim sendLock = new(1, 1);

    private const decimal QuantusUnit = 1_000_000_000_000m;
    private static readonly Regex QtcFreeRegex = new("Free:\\s*([0-9][0-9.,]*)", RegexOptions.Compiled);
    private static readonly Regex QtcTxHashRegex = new("Hash:\\s*(0x[0-9a-fA-F]{64})", RegexOptions.Compiled);

    public class WalletInfo
    {
        public string Coin { get; set; }
        public string Name { get; set; }
        public string[] Pools { get; set; }
        public string Address { get; set; }
        public decimal? Available { get; set; }
        public decimal? Pending { get; set; }
        public decimal? Immature { get; set; }
        public decimal OwedToMiners { get; set; }
        public decimal? Free { get; set; }
        public string ExplorerTxLink { get; set; }
        public string Error { get; set; }
    }

    public class SendRequest
    {
        public string Address { get; set; }
        public decimal Amount { get; set; }
        public string Password { get; set; }
    }

    public class SendResult
    {
        public string TxId { get; set; }
        public string ExplorerUrl { get; set; }
    }

    // coin symbol -> pools paying from that coin's wallet
    private IEnumerable<IGrouping<string, PoolConfig>> WalletGroups() =>
        clusterConfig.Pools
            .Where(p => p.Enabled && p.Template != null)
            .GroupBy(p => p.Template.Symbol.ToUpperInvariant());

    public async Task<WalletInfo[]> GetWalletsAsync(CancellationToken ct)
    {
        var tasks = WalletGroups().Select(g => GetWalletAsync(g.Key, g.ToArray(), ct));
        return (await Task.WhenAll(tasks)).OrderBy(x => x.Coin).ToArray();
    }

    private async Task<WalletInfo> GetWalletAsync(string symbol, PoolConfig[] pools, CancellationToken ct)
    {
        var pc = pools[0];

        var info = new WalletInfo
        {
            Coin = symbol,
            Name = pc.Template.Name,
            Pools = pools.Select(x => x.Id).ToArray(),
            Address = pc.Address,
            ExplorerTxLink = pc.Template.ExplorerTxLink,
        };

        try
        {
            var ids = info.Pools;
            info.OwedToMiners = await cf.Run(con => con.ExecuteScalarAsync<decimal>(
                "SELECT COALESCE(SUM(amount), 0) FROM balances WHERE poolid = ANY(@ids)", new { ids }));
        }

        catch(Exception ex)
        {
            logger.Warn(() => $"Could not sum miner balances for {symbol}: {ex.Message}");
        }

        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(TimeSpan.FromSeconds(30));

            switch(pc.Template.Family)
            {
                case CoinFamily.Bitcoin:
                case CoinFamily.Progpow:
                {
                    var rpc = Rpc(pc, null);
                    var wi = await rpc.ExecuteAsync<JToken>(logger, "getwalletinfo", timeout.Token);

                    if(wi.Error == null && wi.Response != null)
                    {
                        info.Available = wi.Response.Value<decimal?>("balance");
                        info.Pending = wi.Response.Value<decimal?>("unconfirmed_balance");
                        info.Immature = wi.Response.Value<decimal?>("immature_balance");
                    }

                    else
                    {
                        var b = await rpc.ExecuteAsync<JToken>(logger, "getbalance", timeout.Token);

                        if(b.Error != null)
                            throw new Exception(b.Error.Message);

                        info.Available = b.Response.Value<decimal>();
                    }

                    break;
                }

                case CoinFamily.Pearl:
                {
                    var rpc = Rpc(pc, "wallet");
                    var b = await rpc.ExecuteAsync<JToken>(logger, "getbalance", timeout.Token);

                    if(b.Error != null)
                        throw new Exception(b.Error.Message);

                    info.Available = b.Response.Value<decimal>();

                    var u = await rpc.ExecuteAsync<JToken>(logger, "getunconfirmedbalance", timeout.Token);

                    if(u.Error == null && u.Response != null)
                        info.Pending = u.Response.Value<decimal>();

                    break;
                }

                case CoinFamily.Cryptonote:
                {
                    var rpc = Rpc(pc, "wallet");
                    var b = await rpc.ExecuteAsync<JToken>(logger, "get_balance", timeout.Token, new { account_index = 0 });

                    if(b.Error != null)
                        throw new Exception(b.Error.Message);

                    var unit = CryptonoteUnit(pc);
                    var total = b.Response.Value<decimal>("balance") / unit;
                    info.Available = b.Response.Value<decimal>("unlocked_balance") / unit;
                    info.Pending = total - info.Available;
                    break;
                }

                case CoinFamily.Zano:
                {
                    var rpc = Rpc(pc, "wallet");
                    var b = await rpc.ExecuteAsync<GetBalanceResponse>(logger, ZanoWalletCommands.GetBalance, timeout.Token);

                    if(b.Error != null)
                        throw new Exception(b.Error.Message);

                    var unit = ZanoUnit(pc);
                    info.Available = b.Response.UnlockedBalance / unit;
                    info.Pending = (b.Response.Balance - b.Response.UnlockedBalance) / unit;
                    break;
                }

                case CoinFamily.Quantus:
                {
                    var extra = QtcExtra(pc);
                    info.Address = extra.WalletAddress ?? info.Address;

                    var (code, output) = await RunQuantusCliAsync(extra, timeout.Token, "balance", "--address", extra.WalletAddress);
                    var m = QtcFreeRegex.Match(output);

                    if(code != 0 || !m.Success)
                        throw new Exception("quantus balance failed: " + Tail(output));

                    info.Available = decimal.Parse(m.Groups[1].Value.Replace(",", ""), CultureInfo.InvariantCulture);
                    break;
                }

                case CoinFamily.Kaspa when IsZkas(pc):
                {
                    var status = await ZkasStatusAsync(pc, timeout.Token);

                    info.Address = status.Value<string>("address") ?? info.Address;
                    info.Available = ZkasSompi(status["spendable_sompi"]) / KaspaConstants.SmallestUnit;
                    info.Pending = ZkasSompi(status["maturing_sompi"]) / KaspaConstants.SmallestUnit;

                    if(status.Value<bool?>("synced") != true)
                        info.Error = "wallet is still syncing";

                    break;
                }

                case CoinFamily.Kaspa:
                {
                    var wallet = KaspaWallet(pc);
                    var b = await wallet.GetBalanceAsync(new kaspaWalletd.GetBalanceRequest(), cancellationToken: timeout.Token);

                    info.Available = b.Available / KaspaConstants.SmallestUnit;
                    info.Pending = b.Pending / KaspaConstants.SmallestUnit;
                    break;
                }

                default:
                    throw new Exception($"wallet balance not supported for {pc.Template.Family}");
            }
        }

        catch(Exception ex)
        {
            info.Error = ex is OperationCanceledException ? "wallet did not answer in time" : ex.Message;
        }

        if(info.Available.HasValue)
            info.Free = info.Available.Value - info.OwedToMiners;

        return info;
    }

    public async Task<SendResult> SendAsync(string symbol, SendRequest req, string clientIp, CancellationToken ct)
    {
        var group = WalletGroups().FirstOrDefault(g => g.Key == symbol?.ToUpperInvariant())
            ?? throw new ArgumentException("Unknown coin");

        var pc = group.First();
        var address = req.Address?.Trim();
        var amount = req.Amount;

        if(string.IsNullOrEmpty(address))
            throw new ArgumentException("Enter an address");

        if(amount <= 0)
            throw new ArgumentException("Enter an amount greater than 0");

        if(!await sendLock.WaitAsync(TimeSpan.FromSeconds(5), ct))
            throw new InvalidOperationException("Another send is still running. Wait for it to finish.");

        try
        {
            logger.Warn(() => $"Admin send requested from {clientIp}: {amount} {group.Key} to {address}");

            var txid = pc.Template.Family switch
            {
                CoinFamily.Bitcoin or CoinFamily.Progpow => await SendBitcoinAsync(pc, address, amount, ct),
                CoinFamily.Pearl => await SendPearlAsync(pc, address, amount, ct),
                CoinFamily.Zano => await SendZanoAsync(pc, address, amount, ct),
                CoinFamily.Cryptonote => await SendCryptonoteAsync(pc, address, amount, ct),
                CoinFamily.Quantus => await SendQuantusAsync(pc, address, amount, ct),
                CoinFamily.Kaspa when IsZkas(pc) => await SendZkasAsync(pc, address, amount, ct),
                CoinFamily.Kaspa => await SendKaspaAsync(pc, address, amount, ct),
                _ => throw new InvalidOperationException($"sending not supported for {pc.Template.Family}")
            };

            logger.Warn(() => $"Admin send done: {amount} {group.Key} to {address}, tx {txid}");

            messageBus.SendMessage(new AdminNotification($"Admin wallet send: {amount} {group.Key}",
                $"{amount} {group.Key} sent from the pool wallet to {address} by the admin panel ({clientIp}). Transaction: {txid}"));

            var link = pc.Template.ExplorerTxLink;

            return new SendResult
            {
                TxId = txid,
                ExplorerUrl = !string.IsNullOrEmpty(link) && !string.IsNullOrEmpty(txid) ? string.Format(link, txid) : null
            };
        }

        catch(Exception ex) when(ex is not ArgumentException)
        {
            logger.Error(() => $"Admin send failed: {amount} {group.Key} to {address}: {ex.Message}");
            throw;
        }

        finally
        {
            sendLock.Release();
        }
    }

    private async Task<string> SendBitcoinAsync(PoolConfig pc, string address, decimal amount, CancellationToken ct)
    {
        var rpc = Rpc(pc, null);

        var va = await rpc.ExecuteAsync<JToken>(logger, "validateaddress", ct, new object[] { address });

        if(va.Error != null || va.Response?.Value<bool>("isvalid") != true)
            throw new ArgumentException($"Not a valid {pc.Template.Symbol} address");

        var res = await rpc.ExecuteAsync<string>(logger, "sendtoaddress", ct, new object[] { address, Math.Round(amount, 8) });

        if(res.Error != null)
            throw new InvalidOperationException(res.Error.Message);

        return res.Response;
    }

    private async Task<string> SendPearlAsync(PoolConfig pc, string address, decimal amount, CancellationToken ct)
    {
        var extra = JsonConvert.DeserializeObject<PearlPaymentProcessingConfigExtra>(
            JsonConvert.SerializeObject(pc.PaymentProcessing?.Extra)) ?? new PearlPaymentProcessingConfigExtra();

        var rpc = Rpc(pc, "wallet");

        var va = await rpc.ExecuteAsync<JToken>(logger, "validateaddress", ct, new object[] { address });

        if(va.Error != null || va.Response?.Value<bool>("isvalid") != true)
            throw new ArgumentException("Not a valid Pearl address");

        if(string.IsNullOrWhiteSpace(extra.WalletPassphrase))
            throw new InvalidOperationException("The Pearl wallet passphrase is not configured");

        var unlock = await rpc.ExecuteAsync<object>(logger, "walletpassphrase", ct, new object[] { extra.WalletPassphrase, 60 });

        if(unlock.Error != null)
            throw new InvalidOperationException("Could not unlock the Pearl wallet: " + unlock.Error.Message);

        try
        {
            var res = await rpc.ExecuteAsync<string>(logger, "sendfrom", ct,
                new object[] { extra.Account ?? "default", address, Math.Round(amount, 8), extra.FeeRatePerKb, extra.MinConf });

            if(res.Error != null || string.IsNullOrWhiteSpace(res.Response))
                throw new InvalidOperationException(res.Error?.Message ?? "sendfrom returned no transaction id");

            return res.Response;
        }

        finally
        {
            await rpc.ExecuteAsync<object>(logger, "walletlock", CancellationToken.None, Array.Empty<object>());
        }
    }

    private async Task<string> SendZanoAsync(PoolConfig pc, string address, decimal amount, CancellationToken ct)
    {
        if(!address.StartsWith("Z") && !address.StartsWith("iZ"))
            throw new ArgumentException("Not a valid Zano address");

        var rpc = Rpc(pc, "wallet");
        var unit = ZanoUnit(pc);

        var request = new TransferRequest
        {
            Destinations = new[]
            {
                new TransferDestination { Address = address, Amount = (ulong) Math.Floor(amount * unit) }
            },
            Fee = ZanoConstants.MinimumTransactionFee,
            RevealSender = false,
            HideReceiver = true,
        };

        var res = await rpc.ExecuteAsync<TransferResponse>(logger, ZanoWalletCommands.Transfer, ct, request);

        if(res.Error != null)
            throw new InvalidOperationException(res.Error.Message);

        return res.Response?.TxHash;
    }

    private async Task<string> SendCryptonoteAsync(PoolConfig pc, string address, decimal amount, CancellationToken ct)
    {
        var rpc = Rpc(pc, "wallet");

        var va = await rpc.ExecuteAsync<JToken>(logger, "validate_address", ct, new { address });

        if(va.Error != null || va.Response?.Value<bool>("valid") != true)
            throw new ArgumentException($"Not a valid {pc.Template.Symbol} address");

        var request = new
        {
            destinations = new[] { new { address, amount = (ulong) Math.Floor(amount * CryptonoteUnit(pc)) } },
            account_index = 0,
            priority = 0,
            get_tx_key = true
        };

        var res = await rpc.ExecuteAsync<JToken>(logger, "transfer", ct, request);

        if(res.Error != null)
            throw new InvalidOperationException(res.Error.Message);

        return res.Response?.Value<string>("tx_hash");
    }

    private async Task<string> SendQuantusAsync(PoolConfig pc, string address, decimal amount, CancellationToken ct)
    {
        if(!IsValidQuantusAddress(address))
            throw new ArgumentException("Not a valid Quantus address");

        var extra = QtcExtra(pc);

        if(string.IsNullOrEmpty(extra.WalletName) || string.IsNullOrEmpty(extra.WalletPasswordFile))
            throw new InvalidOperationException("The Quantus payout wallet is not configured");

        var file = Path.Combine(Path.GetTempPath(), $"qtc-admin-send-{Guid.NewGuid():N}.json");

        var transfers = new[]
        {
            new
            {
                to = address,
                amount = ((BigInteger) decimal.Truncate(amount * QuantusUnit)).ToString(CultureInfo.InvariantCulture)
            }
        };

        await File.WriteAllTextAsync(file, JsonConvert.SerializeObject(transfers), ct);

        try
        {
            var (code, output) = await RunQuantusCliAsync(extra, ct,
                "batch", "send",
                "--from", extra.WalletName,
                "--password-file", extra.WalletPasswordFile,
                "--batch-file", file,
                "--wait-for-transaction");

            var hash = QtcTxHashRegex.Match(output).Groups[1].Value;

            if(code != 0 || string.IsNullOrEmpty(hash))
                throw new InvalidOperationException("quantus send did not confirm: " + Tail(output) +
                    " (check the wallet balance before trying again, the transfer may still have gone through)");

            return hash;
        }

        finally
        {
            try
            {
                File.Delete(file);
            }

            catch
            {
                // ignored
            }
        }
    }

    #region Kaspa and ZKas

    // ZKas pays through zkas-walletd's REST API; Kaspa through kaspawallet's gRPC daemon
    private static bool IsZkas(PoolConfig pc) => pc.Template.Symbol.Equals("ZKAS", StringComparison.OrdinalIgnoreCase);

    private static DaemonEndpointConfig WalletEndpoint(PoolConfig pc) =>
        pc.Daemons.FirstOrDefault(x => x.Category?.ToLower() == KaspaConstants.WalletDaemonCategory)
        ?? throw new InvalidOperationException($"no wallet daemon configured for {pc.Id}");

    private static KaspaPaymentProcessingConfigExtra KaspaPayExtra(PoolConfig pc) =>
        pc.PaymentProcessing?.Extra?.SafeExtensionDataAs<KaspaPaymentProcessingConfigExtra>() ?? new();

    private static kaspaWalletd.KaspaWalletdRPC.KaspaWalletdRPCClient KaspaWallet(PoolConfig pc)
    {
        var extra = pc.Extra.SafeExtensionDataAs<KaspaPoolConfigExtra>();

        return KaspaClientFactory.CreateKaspaWalletdRPCClient(new[] { WalletEndpoint(pc) },
            extra?.ProtobufWalletRpcServiceName ?? KaspaConstants.ProtobufWalletRpcServiceName);
    }

    private static string CheckKaspaAddress(PoolConfig pc, string address)
    {
        var prefix = pc.Template.As<KaspaCoinTemplate>()?.AddressBech32Prefix ?? "kaspa";
        address = address.ToLowerInvariant();

        if(!address.StartsWith(prefix + ":", StringComparison.Ordinal) || address.Length < prefix.Length + 40)
            throw new ArgumentException($"Not a valid {pc.Template.Symbol} address (it should start with {prefix}:)");

        return address;
    }

    private async Task<string> SendKaspaAsync(PoolConfig pc, string address, decimal amount, CancellationToken ct)
    {
        address = CheckKaspaAddress(pc, address);

        var wallet = KaspaWallet(pc);
        var pay = KaspaPayExtra(pc);

        var create = new kaspaWalletd.CreateUnsignedTransactionsRequest
        {
            Address = address,
            Amount = (ulong) Math.Floor(amount * KaspaConstants.SmallestUnit),
            UseExistingChangeAddress = false,
            IsSendAll = false
        };

        if(!string.IsNullOrEmpty(pay.VersionEnablingMaxFee))
            create.FeePolicy = new kaspaWalletd.FeePolicy { MaxFee = pay.MaxFee ?? 20000 };

        var unsigned = await wallet.CreateUnsignedTransactionsAsync(create, cancellationToken: ct);

        if(unsigned.UnsignedTransactions.Count == 0)
            throw new InvalidOperationException("the wallet did not create a transaction (not enough spendable balance?)");

        var sign = new kaspaWalletd.SignRequest { Password = pay.WalletPassword };
        sign.UnsignedTransactions.Add(unsigned.UnsignedTransactions);
        var signed = await wallet.SignAsync(sign, cancellationToken: ct);

        var broadcast = new kaspaWalletd.BroadcastRequest();
        broadcast.Transactions.Add(signed.SignedTransactions);
        var sent = await wallet.BroadcastAsync(broadcast, cancellationToken: ct);

        if(sent.TxIDs.Count == 0)
            throw new InvalidOperationException("the wallet did not return a transaction id (check the wallet before trying again)");

        return string.Join(",", sent.TxIDs);
    }

    private static HttpClient ZkasClient(PoolConfig pc, TimeSpan timeout)
    {
        var ep = WalletEndpoint(pc);
        var token = KaspaPayExtra(pc).WalletToken;

        if(string.IsNullOrEmpty(token))
            throw new InvalidOperationException($"no walletToken configured for {pc.Id}");

        var http = new HttpClient { BaseAddress = new Uri($"http://{ep.Host}:{ep.Port}"), Timeout = timeout };
        http.DefaultRequestHeaders.Add("X-Wallet-Token", token);
        return http;
    }

    private static decimal ZkasSompi(JToken token) =>
        token == null || token.Type == JTokenType.Null ? 0 : decimal.Parse(token.ToString(), CultureInfo.InvariantCulture);

    private static async Task<JObject> ZkasStatusAsync(PoolConfig pc, CancellationToken ct)
    {
        using var http = ZkasClient(pc, TimeSpan.FromSeconds(30));
        using var response = await http.GetAsync("/api/status", ct);
        var text = await response.Content.ReadAsStringAsync(ct);

        if(!response.IsSuccessStatusCode)
            throw new Exception($"zkas-walletd status failed ({(int) response.StatusCode}): {Tail(text)}");

        return JObject.Parse(text);
    }

    private async Task<string> SendZkasAsync(PoolConfig pc, string address, decimal amount, CancellationToken ct)
    {
        address = CheckKaspaAddress(pc, address);

        var body = new JObject
        {
            // a fresh id per send, so a retried request can never pay twice
            ["request_id"] = $"admin-{pc.Id}-{DateTime.UtcNow:yyyyMMddHHmmss}-{Guid.NewGuid():N}",
            ["payees"] = new JArray(new JObject { ["to"] = address, ["amount_sompi"] = (ulong) Math.Floor(amount * KaspaConstants.SmallestUnit) })
        };

        var maxFee = KaspaPayExtra(pc).MaxFee;
        if(maxFee is > 0)
            body["max_total_fee"] = maxFee.Value;

        // proving a shielded transaction takes a while
        using var http = ZkasClient(pc, TimeSpan.FromMinutes(10));
        using var content = new StringContent(body.ToString(Formatting.None), System.Text.Encoding.UTF8, "application/json");
        using var response = await http.PostAsync("/api/wallet/send_many", content, ct);
        var text = await response.Content.ReadAsStringAsync(ct);

        JObject answer;
        try { answer = JObject.Parse(text); }
        catch { answer = new JObject { ["error"] = Tail(text) }; }

        var txid = (answer["payee_txids"] as JArray)?.FirstOrDefault()?.Value<string>("txid");

        if(!string.IsNullOrEmpty(txid))
            return txid;

        if(answer.Value<bool?>("outcome_unknown") == true)
            throw new InvalidOperationException($"zkas-walletd could not confirm transaction {answer.Value<string>("txid")}" +
                " (check the wallet history before trying again, it may still have gone through)");

        throw new InvalidOperationException($"zkas-walletd refused the send ({(int) response.StatusCode}): {answer.Value<string>("error") ?? Tail(text)}");
    }

    #endregion

    private RpcClient Rpc(PoolConfig pc, string category)
    {
        var ep = category == null
            ? pc.Daemons.First(x => string.IsNullOrEmpty(x.Category))
            : pc.Daemons.FirstOrDefault(x => x.Category == category)
              ?? throw new InvalidOperationException($"no {category} daemon configured for {pc.Id}");

        return new RpcClient(ep, serializerSettings, messageBus, pc.Id);
    }

    private static decimal CryptonoteUnit(PoolConfig pc)
    {
        var unit = pc.Template.As<CryptonoteCoinTemplate>()?.SmallestUnit ?? 0;
        return unit > 0 ? unit : 1_000_000_000_000m;
    }

    private static decimal ZanoUnit(PoolConfig pc)
    {
        var unit = pc.Template.As<ZanoCoinTemplate>()?.SmallestUnit ?? 0;
        return unit > 0 ? unit : 1_000_000_000_000m;
    }

    private static QuantusPaymentProcessingConfigExtra QtcExtra(PoolConfig pc) =>
        pc.PaymentProcessing?.Extra?.SafeExtensionDataAs<QuantusPaymentProcessingConfigExtra>() ?? new();

    private static string Tail(string output) =>
        string.Join(" | ", (output ?? "").Split('\n').Where(l => !string.IsNullOrWhiteSpace(l)).TakeLast(3));

    private static bool IsValidQuantusAddress(string address)
    {
        try
        {
            var data = NBitcoin.DataEncoders.Encoders.Base58.DecodeData(address);
            return data.Length == 36 && data[0] >= 64 &&
                (((data[0] & 0x3f) << 2) | (data[1] >> 6) | ((data[1] & 0x3f) << 8)) == QuantusConstants.Ss58Prefix;
        }

        catch
        {
            return false;
        }
    }

    private static async Task<(int ExitCode, string Output)> RunQuantusCliAsync(QuantusPaymentProcessingConfigExtra extra, CancellationToken ct, params string[] args)
    {
        var psi = new ProcessStartInfo(extra.CliPath ?? "/usr/local/bin/quantus")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = Path.GetTempPath(),
        };

        foreach(var arg in args)
            psi.ArgumentList.Add(arg);

        psi.ArgumentList.Add("--node-url");
        psi.ArgumentList.Add(extra.NodeUrl ?? "ws://127.0.0.1:9944");
        psi.Environment["NO_COLOR"] = "1";

        using var process = Process.Start(psi) ?? throw new InvalidOperationException("could not start the quantus cli");
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromMinutes(extra.CliTimeoutMinutes ?? 10));

        var stdout = process.StandardOutput.ReadToEndAsync(timeout.Token);
        var stderr = process.StandardError.ReadToEndAsync(timeout.Token);

        try
        {
            await process.WaitForExitAsync(timeout.Token);
        }

        catch(OperationCanceledException)
        {
            try
            {
                process.Kill(true);
            }

            catch
            {
                // ignored
            }

            return (-1, "timed out");
        }

        var output = Regex.Replace(await stdout + "\n" + await stderr, "\u001b\\[[0-9;]*m", "");
        return (process.ExitCode, output);
    }
}
