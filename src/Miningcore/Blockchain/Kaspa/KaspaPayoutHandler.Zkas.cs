using System.Net.Http;
using Grpc.Core;
using System.Text;
using Miningcore.Blockchain.Kaspa.Configuration;
using Miningcore.Configuration;
using Miningcore.Extensions;
using Miningcore.Messaging;
using Miningcore.Notifications.Messages;
using Miningcore.Persistence.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Block = Miningcore.Persistence.Model.Block;
using static Miningcore.Util.ActionUtils;
using kaspad = Miningcore.Blockchain.Kaspa.Kaspad;

namespace Miningcore.Blockchain.Kaspa;

// ZKas (ZKAS) is a Kaspa fork whose every output is shielded (Orchard). Mining is unchanged, but
// two things the Kaspa payout code relies on do not exist there:
//
//  - Rewards are not coinbase outputs of the merging block. They accrue in a slot per payout
//    address and are minted as one shielded note every hundred blocks or so, so no output
//    belongs to one block. A block's colour and depth come from GetBlockRewardInfo, and its
//    reward is the subsidy recorded in its own coinbase payload (fees are not attributed; they
//    stay in the pool wallet and go towards payout fees).
//  - There is no kaspawallet. Payouts go through zkas-walletd's REST API (send_many), which
//    builds, proves and broadcasts the shielded transactions.
public partial class KaspaPayoutHandler
{
    private bool isZkas;
    private HttpClient zkasWallet;

    // ~10 minutes at one block a second, which is also when a mined note becomes spendable
    private const int ZkasMinimumConfirmations = 800;
    // a block nobody has merged after this long is not going to be
    private static readonly TimeSpan ZkasUnmergedTimeout = TimeSpan.FromHours(2);

    private void ZkasConfigure(DaemonEndpointConfig[] walletDaemonEndpoints)
    {
        var endpoint = walletDaemonEndpoints.First();
        var token = extraPoolPaymentProcessingConfig?.WalletToken;

        if(string.IsNullOrEmpty(token))
            throw new PaymentException("ZKas payouts need paymentProcessing.walletToken (the zkas-walletd wallet token of the pool wallet)");

        zkasWallet = new HttpClient
        {
            BaseAddress = new Uri($"http://{endpoint.Host}:{endpoint.Port}"),
            // proving a large payout takes minutes
            Timeout = TimeSpan.FromMinutes(45)
        };
        zkasWallet.DefaultRequestHeaders.Add("X-Wallet-Token", token);
    }

    private async Task<JObject> ZkasWalletStatusAsync(CancellationToken ct)
    {
        using var response = await zkasWallet.GetAsync("/api/status", ct);
        var text = await response.Content.ReadAsStringAsync(ct);

        if(!response.IsSuccessStatusCode)
            throw new PaymentException($"zkas-walletd status failed with {(int) response.StatusCode}: {Cut(text, 200)}");

        return JObject.Parse(text);
    }

    private static string Cut(string text, int max)
    {
        return text == null || text.Length <= max ? text : text.Substring(0, max);
    }

    private static decimal ZkasSompi(JToken token)
    {
        // amounts arrive as decimal strings (or numbers) of sompi
        return token == null || token.Type == JTokenType.Null ? 0 : decimal.Parse(token.ToString(), System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Classifies one pending ZKas block. Returns false when it is still too young to decide.
    /// </summary>
    private async Task<bool> ZkasClassifyBlockAsync(AsyncDuplexStreamingCall<kaspad.KaspadMessage, kaspad.KaspadMessage> stream, Block block, kaspad.RpcBlock rpcBlock, KaspaCoinTemplate coin, int minConfirmations, CancellationToken ct)
    {
        var request = new kaspad.KaspadMessage
        {
            GetBlockRewardInfoRequest = new kaspad.GetBlockRewardInfoRequestMessage { Hash = block.Hash }
        };
        await Guard(() => stream.RequestStream.WriteAsync(request), ex => logger.Debug(ex));

        kaspad.GetBlockRewardInfoResponseMessage info = null;
        await foreach(var response in stream.ResponseStream.ReadAllAsync(ct))
        {
            info = response.GetBlockRewardInfoResponse;
            break;
        }

        void Orphan(string why)
        {
            block.Status = BlockStatus.Orphaned;
            block.Reward = 0;
            logger.Info(() => $"[{LogCategory}] Block {block.BlockHeight} classified as orphaned because {why}");
            messageBus.NotifyBlockUnlocked(poolConfig.Id, block, coin);
        }

        if(info == null || !string.IsNullOrEmpty(info.Error?.Message))
        {
            Orphan($"the node does not know it ({info?.Error?.Message ?? "no answer"})");
            return true;
        }

        var confirmations = info.HasConfirmationCount ? info.ConfirmationCount : 0;

        if(info.BlockColor == kaspad.BlockColor.Unknown)
        {
            // not merged by a chain block yet
            if(clock.Now - block.Created > ZkasUnmergedTimeout)
            {
                Orphan("no chain block merged it");
                return true;
            }

            block.ConfirmationProgress = 0;
            messageBus.NotifyBlockConfirmationProgress(poolConfig.Id, block, coin);
            return false;
        }

        block.ConfirmationProgress = Math.Min(1.0d, (double) confirmations / minConfirmations);
        messageBus.NotifyBlockConfirmationProgress(poolConfig.Id, block, coin);

        // colours can still flip in a reorg until the merging block is buried
        if(block.ConfirmationProgress < 1)
            return false;

        if(info.BlockColor == kaspad.BlockColor.Red)
        {
            Orphan("it is a red block, which earns nothing");
            return true;
        }

        // blue: the reward is the subsidy written in the block's own coinbase payload
        // (blue score: 8 bytes, subsidy: 8 bytes little-endian, ...)
        var coinbase = rpcBlock?.Transactions?.FirstOrDefault(x => x.Inputs.Count < 1);
        var payload = coinbase?.Payload?.HexToByteArray();

        if(payload == null || payload.Length < 16)
        {
            logger.Warn(() => $"[{LogCategory}] Block {block.BlockHeight} is blue but its coinbase payload could not be read; leaving it pending");
            return false;
        }

        var subsidy = BitConverter.ToUInt64(payload, 8);

        block.Status = BlockStatus.Confirmed;
        block.ConfirmationProgress = 1;
        block.Reward = subsidy / KaspaConstants.SmallestUnit;

        logger.Info(() => $"[{LogCategory}] Unlocked block {block.BlockHeight} worth {FormatAmount(block.Reward)}");
        messageBus.NotifyBlockUnlocked(poolConfig.Id, block, coin);

        return true;
    }

    private async Task ZkasPayoutAsync(Dictionary<string, decimal> amounts, CancellationToken ct)
    {
        // whole sompi only; whatever is left of a balance stays for the next round
        var payees = amounts
            .Select(x => (Address: x.Key, Sompi: (ulong) Math.Floor(x.Value * KaspaConstants.SmallestUnit)))
            .Where(x => x.Sompi > 0)
            .ToArray();

        if(payees.Length == 0)
            return;

        var total = payees.Sum(x => (decimal) x.Sompi) / KaspaConstants.SmallestUnit;

        var status = await ZkasWalletStatusAsync(ct);
        var spendable = ZkasSompi(status["spendable_sompi"]) / KaspaConstants.SmallestUnit;
        var maturing = ZkasSompi(status["maturing_sompi"]) / KaspaConstants.SmallestUnit;

        logger.Info(() => $"[{LogCategory}] Current wallet balance - Spendable: [{FormatAmount(spendable)}] - Maturing: [{FormatAmount(maturing)}] - Notes: {status["note_count"]}");

        if(status.Value<bool?>("synced") != true || status.Value<bool?>("spend_ready") == false)
        {
            logger.Warn(() => $"[{LogCategory}] Wallet is not synced and ready to spend yet. Will try again");
            return;
        }

        // leave room for the transaction fees, which the pool pays
        var feeReserve = Math.Max(1.0m, 0.25m * (decimal) Math.Ceiling(payees.Length / 37.0));

        if(spendable < total + feeReserve)
        {
            // Rewards reach the wallet in batches and need time to mature, so it can trail what miners
            // are owed. Pay as many as it covers now, smallest first; the others wait for the next round.
            var budget = spendable - feeReserve;
            var covered = new List<(string Address, ulong Sompi)>();

            foreach(var payee in payees.OrderBy(x => x.Sompi))
            {
                var amount = payee.Sompi / KaspaConstants.SmallestUnit;
                if(amount > budget)
                    break;

                budget -= amount;
                covered.Add(payee);
            }

            if(covered.Count == 0)
            {
                logger.Warn(() => $"[{LogCategory}] Wallet balance currently short of {FormatAmount(total + feeReserve - spendable)}. Will try again");
                return;
            }

            logger.Info(() => $"[{LogCategory}] Wallet covers {covered.Count} of {payees.Length} payouts for now. The others wait for maturing rewards");

            payees = covered.ToArray();
            total = payees.Sum(x => (decimal) x.Sompi) / KaspaConstants.SmallestUnit;
        }

        var body = new JObject
        {
            // a fresh id per run: a retry of this very request (a dropped connection) cannot pay twice
            ["request_id"] = Cut($"mc-{poolConfig.Id}-{clock.Now:yyyyMMddHHmmss}-{Guid.NewGuid():N}", 120),
            ["payees"] = new JArray(payees.Select(x => new JObject { ["to"] = x.Address, ["amount_sompi"] = x.Sompi }))
        };

        if(extraPoolPaymentProcessingConfig?.MaxFee is > 0)
            body["max_total_fee"] = extraPoolPaymentProcessingConfig.MaxFee.Value;

        logger.Info(() => $"[{LogCategory}] Paying {FormatAmount(total)} to {payees.Length} addresses through zkas-walletd");

        using var content = new StringContent(body.ToString(Formatting.None), Encoding.UTF8, "application/json");
        using var response = await zkasWallet.PostAsync("/api/wallet/send_many", content, ct);
        var text = await response.Content.ReadAsStringAsync(ct);
        var code = (int) response.StatusCode;

        JObject answer;
        try
        {
            answer = JObject.Parse(text);
        }
        catch
        {
            answer = new JObject { ["error"] = Cut(text, 300) };
        }

        var payeeTxids = answer["payee_txids"] as JArray;
        var outcomeUnknown = answer.Value<bool?>("outcome_unknown") == true;

        // nothing went out: 4xx (bad request, fee over the limit, wallet not ready), 500, 503
        if(payeeTxids == null && !outcomeUnknown)
        {
            var error = $"zkas-walletd refused the payout ({code}): {answer.Value<string>("error") ?? Cut(text, 300)}";
            logger.Error(() => $"[{LogCategory}] {error}");
            NotifyPayoutFailure(poolConfig.Id, payees.Select(x => new Balance { Amount = x.Sompi / KaspaConstants.SmallestUnit }).ToArray(), error, null);
            return;
        }

        var paid = new Dictionary<Balance, string>();
        var unpaid = new List<(string Address, ulong Sompi)>();

        for(var i = 0; i < payees.Length; i++)
        {
            // one entry per payee, in request order
            var txid = payeeTxids != null && i < payeeTxids.Count ? payeeTxids[i].Value<string>("txid") : null;

            if(string.IsNullOrEmpty(txid) && outcomeUnknown)
            {
                // The wallet could not learn whether a transaction landed. Paying these again could
                // pay twice, so they are recorded as paid under the transaction in doubt and flagged
                // for the operator, who can settle from the wallet's history if it never confirms.
                txid = answer.Value<string>("txid") ?? "unknown";
            }

            if(string.IsNullOrEmpty(txid))
                unpaid.Add(payees[i]);
            else
                paid.Add(new Balance { PoolId = poolConfig.Id, Address = payees[i].Address, Amount = payees[i].Sompi / KaspaConstants.SmallestUnit }, txid);
        }

        if(paid.Count > 0)
        {
            await PersistPaymentsAsync(paid);
            NotifyPayoutSuccess(poolConfig.Id, paid.Keys.ToArray(), paid.Values.Distinct().ToArray(), null);
            logger.Info(() => $"[{LogCategory}] Paid {paid.Count} addresses in {paid.Values.Distinct().Count()} transaction(s), fees {FormatAmount(ZkasSompi(answer["fee_sompi"]) / KaspaConstants.SmallestUnit)}");
        }

        if(outcomeUnknown)
        {
            var warning = $"zkas-walletd could not confirm transaction {answer.Value<string>("txid")}: the payees in doubt were recorded as paid. Check the wallet history and settle by hand if it never confirms.";
            logger.Error(() => $"[{LogCategory}] {warning}");
            messageBus.SendMessage(new AdminNotification("ZKas payout outcome unknown", $"Pool {poolConfig.Id}: {warning}"));
        }

        if(unpaid.Count > 0)
        {
            var error = $"zkas-walletd paid only part of the run ({code}): {answer.Value<string>("error") ?? "no reason given"}; {unpaid.Count} addresses will be retried";
            logger.Error(() => $"[{LogCategory}] {error}");
            NotifyPayoutFailure(poolConfig.Id, unpaid.Select(x => new Balance { Amount = x.Sompi / KaspaConstants.SmallestUnit }).ToArray(), error, null);
        }
    }
}
