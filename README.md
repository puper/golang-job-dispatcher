# golang-job-dispatcher
异步任务分发系统
- 消息持久化到leveldb
- http任务put接口
- json rpc推到处理接口
- 保证局部有序，相同同key的消息保证顺序执行