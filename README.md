# golang-job-dispatcher
异步任务分发系统
- 消息持久化到leveldb
- http任务put接口
- json rpc推到处理接口
- 保证局部有序，相同key的消息保证顺序执行

# 使用方法
- post http://address/put 格式［form］: type=xxx key=xxx data=xxxx
- curl curl -i -X POST -H "Content-Type:application/x-www-form-urlencoded" -d "type=test" -d "key=" -d "data=fffffff" 'http://127.0.0.1:8081/put'

- 暂停 post http://address/command/pause 后端暂时不可用时可以接受消息但不推送
- 继续 post http://address/command/continue 后端可用时继续推送
- 重新加载配置文件 post http://address/command/reload (只能重新加载rules)



# 场景
- 对局部顺序有要求，不恰当的例子：异步点赞和取消赞，快速操作，如果不保证顺序，最终结果可能和预期不一致

# TODO
- 暂停处理消息
- 热更新？（重启得中断服务）
- 日志记录
- 状态统计
- 错误报警
- 优化
