import React from 'react';

const HttpLifecycleTimeline = () => {
  // 定义生命周期阶段
  const stages = [
    { id: 1, name: '客户端与Nginx建立TCP连接', short: 'TCP连接' },
    { id: 2, name: '客户端发送HTTP请求到Nginx', short: '发送请求' },
    { id: 3, name: 'Nginx与后端服务建立连接', short: '后端连接' },
    { id: 4, name: 'Nginx转发请求到后端服务', short: '请求转发' },
    { id: 5, name: '后端服务处理业务逻辑', short: '业务处理' },
    { id: 6, name: '后端服务生成并返回响应头', short: '响应头' },
    { id: 7, name: '后端服务传输完整响应体', short: '响应体' },
    { id: 8, name: 'Nginx接收完整后端响应', short: 'Nginx接收' },
    { id: 9, name: 'Nginx向客户端传输响应数据', short: '向客户端传输' },
    { id: 10, name: '客户端接收完整响应', short: '客户端接收' },
    { id: 11, name: 'Nginx与客户端断开TCP连接', short: 'TCP断开' }
  ];

  // 定义时间参数和对应的阶段范围
  const timeMetrics = [
    {
      name: 'upstream_connect_time',
      displayName: '后端连接时长',
      stages: [3],
      color: 'bg-red-200 border-red-400',
      description: '与后端服务建立连接的时间'
    },
    {
      name: 'upstream_header_time',
      displayName: '后端处理时长',
      stages: [3, 4, 5, 6],
      color: 'bg-green-200 border-green-400',
      description: '从连接建立到收到响应头的时间'
    },
    {
      name: 'upstream_response_time',
      displayName: '后端响应时长',
      stages: [3, 4, 5, 6, 7],
      color: 'bg-blue-200 border-blue-400',
      description: '后端服务完整响应时间'
    },
    {
      name: 'total_request_duration',
      displayName: '请求总时长',
      stages: [2, 3, 4, 5, 6, 7, 8, 9, 10],
      color: 'bg-purple-200 border-purple-400',
      description: 'Nginx处理完整请求的总时间'
    }
  ];

  // 定义阶段参数
  const phaseMetrics = [
    {
      name: 'backend_connect_phase',
      displayName: '后端连接阶段',
      stages: [3],
      color: 'bg-orange-300 border-orange-500',
      formula: 'upstream_connect_time'
    },
    {
      name: 'backend_process_phase',
      displayName: '后端处理阶段',
      stages: [4, 5, 6],
      color: 'bg-emerald-300 border-emerald-500',
      formula: 'upstream_header_time - upstream_connect_time'
    },
    {
      name: 'backend_transfer_phase',
      displayName: '后端传输阶段',
      stages: [7],
      color: 'bg-sky-300 border-sky-500',
      formula: 'upstream_response_time - upstream_header_time'
    },
    {
      name: 'nginx_transfer_phase',
      displayName: 'Nginx传输阶段',
      stages: [8, 9, 10],
      color: 'bg-amber-300 border-amber-500',
      formula: 'total_request_duration - upstream_response_time'
    }
  ];

  const renderTimelineRow = (metric, index) => {
    return (
      <div key={metric.name} className="flex items-center mb-3">
        {/* 参数名称 */}
        <div className="w-56 text-sm font-medium text-gray-700 pr-4">
          <div className="font-semibold">{metric.displayName}</div>
          <div className="text-xs text-gray-500 mt-1">
            {metric.description || metric.formula}
          </div>
        </div>
        
        {/* 时间轴 */}
        <div className="flex-1 flex">
          {stages.map((stage) => {
            const isActive = metric.stages.includes(stage.id);
            return (
              <div
                key={stage.id}
                className={`
                  flex-1 h-8 border-2 border-gray-300 mx-0.5 flex items-center justify-center text-xs font-medium
                  ${isActive ? metric.color : 'bg-gray-50'}
                  ${isActive ? 'opacity-90' : 'opacity-30'}
                `}
                title={`${stage.id}. ${stage.name}`}
              >
                {stage.id}
              </div>
            );
          })}
        </div>
      </div>
    );
  };

  return (
    <div className="p-6 bg-white">
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-800 mb-2">HTTP请求生命周期时序图</h1>
        <p className="text-gray-600">基于Nginx日志的完整请求链路分析，每个数字代表一个处理阶段</p>
      </div>

      {/* 阶段说明 */}
      <div className="mb-8 p-4 bg-gray-50 rounded-lg">
        <h3 className="font-semibold text-gray-800 mb-3">处理阶段说明</h3>
        <div className="grid grid-cols-2 lg:grid-cols-3 gap-2 text-sm">
          {stages.map((stage) => (
            <div key={stage.id} className="flex items-center">
              <span className="w-6 h-6 bg-blue-100 border border-blue-300 rounded flex items-center justify-center text-xs font-bold mr-2">
                {stage.id}
              </span>
              <span className="text-gray-700">{stage.short}</span>
            </div>
          ))}
        </div>
      </div>

      {/* 基础时间参数 */}
      <div className="mb-8">
        <h3 className="text-lg font-semibold text-gray-800 mb-4 flex items-center">
          <div className="w-4 h-4 bg-blue-500 rounded-full mr-2"></div>
          基础时间参数 (Nginx原生指标)
        </h3>
        {timeMetrics.map((metric, index) => renderTimelineRow(metric, index))}
      </div>

      {/* 阶段时间参数 */}
      <div className="mb-8">
        <h3 className="text-lg font-semibold text-gray-800 mb-4 flex items-center">
          <div className="w-4 h-4 bg-green-500 rounded-full mr-2"></div>
          阶段时间参数 (衍生计算指标)
        </h3>
        {phaseMetrics.map((metric, index) => renderTimelineRow(metric, index))}
      </div>

      {/* 图例说明 */}
      <div className="mt-6 p-4 bg-blue-50 rounded-lg">
        <h4 className="font-semibold text-gray-800 mb-2">图例说明</h4>
        <div className="text-sm text-gray-700 space-y-1">
          <div>• <strong>有色区块</strong>：该时间参数所覆盖的处理阶段</div>
          <div>• <strong>灰色区块</strong>：该时间参数未涉及的阶段</div>
          <div>• <strong>基础时间参数</strong>：Nginx日志直接提供的原始时间数据</div>
          <div>• <strong>阶段时间参数</strong>：通过基础参数计算得出的具体阶段耗时</div>
        </div>
      </div>

      {/* 性能分析要点 */}
      <div className="mt-6 p-4 bg-yellow-50 rounded-lg">
        <h4 className="font-semibold text-gray-800 mb-2">性能分析要点</h4>
        <div className="text-sm text-gray-700 space-y-1">
          <div>• <strong>后端连接阶段</strong>异常 → 检查网络质量和连接池配置</div>
          <div>• <strong>后端处理阶段</strong>异常 → 检查业务逻辑和数据库性能</div>
          <div>• <strong>后端传输阶段</strong>异常 → 检查响应数据大小和网络带宽</div>
          <div>• <strong>Nginx传输阶段</strong>异常 → 检查Nginx配置和客户端网络</div>
        </div>
      </div>
    </div>
  );
};

export default HttpLifecycleTimeline;