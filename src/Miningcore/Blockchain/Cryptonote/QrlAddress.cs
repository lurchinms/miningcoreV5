using System.Security.Cryptography;

namespace Miningcore.Blockchain.Cryptonote;

/// <summary>
/// QRL addresses: 'Q' + hex of [3-byte descriptor | 32-byte hash | 4-byte checksum],
/// where checksum = last 4 bytes of SHA2-256(descriptor | hash).
/// </summary>
public static class QrlAddress
{
    public const int DescriptorSize = 3;
    public const int HashSize = 32;
    public const int ChecksumSize = 4;
    public const int ByteLength = DescriptorSize + HashSize + ChecksumSize; // 39

    public static bool IsValid(string address)
    {
        if(string.IsNullOrEmpty(address) || address.Length != 1 + ByteLength * 2 || address[0] != 'Q')
            return false;

        byte[] raw;

        try
        {
            raw = Convert.FromHexString(address.AsSpan(1));
        }

        catch(FormatException)
        {
            return false;
        }

        var digest = SHA256.HashData(raw.AsSpan(0, DescriptorSize + HashSize));

        return digest.AsSpan(digest.Length - ChecksumSize).SequenceEqual(raw.AsSpan(DescriptorSize + HashSize, ChecksumSize));
    }
}
