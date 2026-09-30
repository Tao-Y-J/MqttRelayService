using System;
using Microsoft.Extensions.DependencyInjection;

namespace MqttRelayService.Utilities
{
    /// <summary>
    /// 延迟解析的服务包装器，用于打断装饰器与被装饰依赖之间的构造期循环依赖。
    /// 边界：装饰器只能在构造期保存本包装器，必须在首次真正使用（业务调用发生）时才读取 <see cref="Value"/>；
    /// 启动期解析只会构造包装器，不会触发被延迟服务构造，因此不会形成构造环。
    /// </summary>
    /// <typeparam name="T">被延迟解析的服务类型</typeparam>
    public sealed class LazyService<T> where T : class
    {
        private readonly IServiceProvider? _serviceProvider;
        private readonly T? _instance;
        private T? _resolved;

        /// <summary>
        /// 以容器为来源构造延迟解析器，第一次读取 <see cref="Value"/> 时才向容器请求实例。
        /// 这是本类型唯一的公开构造函数：存在第二个可从容器解析的构造函数会让容器无法选择。
        /// </summary>
        public LazyService(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        private LazyService(T instance)
        {
            _instance = instance;
        }

        /// <summary>
        /// 以固定实例构造延迟解析器，供单元测试与不依赖容器的场景使用。
        /// </summary>
        public static LazyService<T> From(T instance)
        {
            return new LazyService<T>(instance);
        }

        /// <summary>
        /// 目标服务实例。已在容器中注册为单例时，这里取到的就是同一个实例。
        /// </summary>
        public T Value => _resolved ??= (_instance ?? _serviceProvider!.GetRequiredService<T>());
    }
}
