# MIT 6.824 2023 by fengwanjie
## 过程中踩的坑/难点
### lab1 
1. 我的临时文件最开始是放在src/mr目录下，在使用main程序时运行代码时当前目录为"src/main",程序能够正常运行。但在使用测试文件时，运行过程
中当前目录 为"src/main/mr-tmp"，会出现找不到目录的错误。
2. 在将调用mapf将所有收集到的中间值保存到文件时，可以先在内存中创建nreduce个桶，然后再将这些值先映射入桶中，最后再将这些值存入中间文件。

### lab2A

<img src="https://github.com/fravenx/oss/blob/master/img/%E6%88%AA%E5%B1%8F2023-09-07%2018.24.32.png" alt="截屏2023-09-07 18.24.32" style="zoom:50%;" />

1. 什么时候重置选举的计时器？  

   (1) 选举计时器时间到时，节点状态变为candidate，重置计时器  

   (2) 节点收到RequestVote的RPC请求时，若给予投票，则重置计时器  

   (3) 节点收到leader有效的heartbeat请求时(arg.Term > rf.term)              

2. 并发控制，对于互斥锁如何设计？   

   (1) 处理rpc方法时，可在一开始直接尝试换取锁，并紧接着使用defer释放锁   

   (2) 在调用rpc之前，要先释放锁，不然调用rpc请求可能会有较长延时，长时间占用锁不释放掉可不太好

3. 

<img src="https://github.com/fravenx/oss/blob/master/img/%E6%88%AA%E5%B1%8F2023-09-07%2018.34.58.png" alt="截屏2023-09-07 18.34.58" style="zoom:40%;" />

在该领导选举计时器方法中，若节点的状态为leader要退出时，记得用break语句而不是return语句，否则该节点的领导选举计时器会失效，且之后不再尝试成为leader😃

4. 节点收到发送的rpc请求回复时，要对比该请求发送时的args.Term是否和节点此时的Term相同，若不相同，该回复已过期。

### lab2B
1. 

   

   
