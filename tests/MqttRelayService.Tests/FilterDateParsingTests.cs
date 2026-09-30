using System;
using System.Globalization;
using Xunit;

namespace MqttRelayService.Tests
{
    /// <summary>
    /// 审计查询日期参数解析测试。
    /// 筛选时间统一按服务器本机时区解释：审计表的 CreatedAt/UpdatedAt 使用本机时间写入，
    /// 若带时区偏移的输入不换算成本机时间，跨时区运维会拿到"看似过滤了、其实过滤错区间"的结果。
    /// </summary>
    public class FilterDateParsingTests
    {
        [Fact]
        public void TryParseFilterDate_EmptyInput_ShouldReturnTrueWithoutValue()
        {
            Assert.True(Program.TryParseFilterDate(null, out var value));
            Assert.Null(value);

            Assert.True(Program.TryParseFilterDate(string.Empty, out var emptyValue));
            Assert.Null(emptyValue);
        }

        [Fact]
        public void TryParseFilterDate_LocalTimeInput_ShouldKeepWallClockValueAsLocal()
        {
            Assert.True(Program.TryParseFilterDate("2026-10-01 08:30:00", out var value));

            Assert.NotNull(value);
            Assert.Equal(new DateTime(2026, 10, 1, 8, 30, 0), value!.Value);
            Assert.Equal(DateTimeKind.Local, value.Value.Kind);
        }

        [Fact]
        public void TryParseFilterDate_UtcInput_ShouldConvertToLocalTime()
        {
            var utcInput = "2026-10-01T00:00:00Z";
            var expected = DateTime.Parse(utcInput, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal)
                .ToLocalTime();

            Assert.True(Program.TryParseFilterDate(utcInput, out var value));

            Assert.NotNull(value);
            Assert.Equal(expected, value!.Value);
        }

        [Fact]
        public void TryParseFilterDate_UnparsableInput_ShouldReturnFalse()
        {
            // 解析失败必须让调用方返回 400，不能让调用方以为已经按时间过滤
            Assert.False(Program.TryParseFilterDate("not-a-date", out var value));
            Assert.Null(value);
        }
    }
}
