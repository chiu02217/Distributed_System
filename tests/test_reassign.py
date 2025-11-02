from src.afs.coordinator.coordinator_server import CoordinatorServicer

print("✅ Test file loaded successfully!")  # 调试用

# 创建 Coordinator 对象（不会真的连 AFS）
c = CoordinatorServicer('localhost:8000')

# 模拟任务队列
c.task_queue.put("input_001.txt")
c.task_queue.put("input_002.txt")
c.task_queue.put("input_003.txt")
c.total_tasks = 3

print("\n✅ 初始状态:")
print("任务队列:", list(c.task_queue.queue))
print("已分配任务:", c.assigned_tasks)

# 模拟 worker-1 领取任务
fake_request = type("req", (), {"worker_id": "worker-1"})()
task = c.GetTask(fake_request, None)
print("\n📤 Worker-1 领取任务:", task.filename)
print("任务队列:", list(c.task_queue.queue))
print("已分配任务:", c.assigned_tasks)

# 模拟 worker-1 掉线
print("\n⚠️ 模拟 worker-1 掉线...")
c.reassign_task("worker-1")
print("任务队列:", list(c.task_queue.queue))
print("已分配任务:", c.assigned_tasks)

# 模拟 worker-2 重新领取任务
fake_request2 = type("req", (), {"worker_id": "worker-2"})()
task2 = c.GetTask(fake_request2, None)
print("\n📤 Worker-2 重新领取任务:", task2.filename)
print("任务队列:", list(c.task_queue.queue))
print("已分配任务:", c.assigned_tasks)

print("\n✅ 测试执行完毕！")
