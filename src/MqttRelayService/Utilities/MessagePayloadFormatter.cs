using System.Text;

namespace MqttRelayService.Utilities;

/// <summary>
/// 消息负载格式化工具
/// </summary>
public static class MessagePayloadFormatter
{
    /// <summary>
    /// 将字节数组格式化为可读的字符串摘要
    /// </summary>
    public static string Format(byte[] payload, int maxLength = 200)
    {
        if (payload == null || payload.Length == 0)
        {
            return "[empty]";
        }

        try
        {
            var text = Encoding.UTF8.GetString(payload);
            if (text.Length > maxLength)
            {
                text = text[..maxLength] + "...";
            }
            return text;
        }
        catch
        {
            return $"[binary:{payload.Length}bytes]";
        }
    }

    /// <summary>
    /// 将字节数组转为 Base64
    /// </summary>
    public static string ToBase64(byte[] payload)
    {
        return payload?.Length > 0 ? Convert.ToBase64String(payload) : string.Empty;
    }
}