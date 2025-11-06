from src.afs.coordinator.coordinator_server import CoordinatorServicer

print("✅ Test file loaded successfully!")


c = CoordinatorServicer()

print("✅ Coordinator created")

# 模拟任务队列
c.task_queue.put('input_001.txt')
c.task_queue.put('input_002.txt')
c.task_queue.put('input_003.txt')
print("任务队列:", list(c.task_queue.queue))

# Worker 请求任务
req = type('Req', (), {'worker_id': 'worker-1'})
res = c.GetTask(req, None)
print("Worker-1 获得任务:", res.filename)

# 模拟 Worker 掉线
print("⚠️ 模拟 worker-1 掉线...")
c.reassign_task('worker-1')

# 再让另一个 worker 领取任务
req2 = type('Req', (), {'worker_id': 'worker-2'})
res2 = c.GetTask(req2, None)
print("Worker-2 重新领取任务:", res2.filename)

print("✅ 测试执行完毕！")
