# MIT 6.824 2023 by fengwanjie

## 实验结果

### lab1(10次)

<img src="https://github.com/fravenx/oss/blob/master/img/mit6.824/%E6%88%AA%E5%B1%8F2023-09-13%2016.14.53.png" alt="截屏2023-09-07 18.24.32" style="zoom:50%;" />

### 2A(2000次0fail)

<img src="https://github.com/fravenx/oss/blob/master/img/%E6%88%AA%E5%B1%8F2023-09-07%2018.24.32.png" alt="截屏2023-09-07 18.24.32" style="zoom:50%;" />

### 2B(2000次0fail)

<img src="https://github.com/fravenx/oss/blob/master/img/mit6.824/%E6%88%AA%E5%B1%8F2023-09-13%2015.55.33.png" alt="截屏2023-09-07 18.24.32" style="zoom:50%;" />



## 过程中踩的坑/难点（除了严格遵守论文图2）
### lab1 
1. 我的临时文件最开始是放在src/mr目录下，在使用main程序时运行代码时当前目录为"src/main",程序能够正常运行。但在使用测试文件时，运行过程
中当前目录 为"src/main/mr-tmp"，会出现找不到目录的错误。
2. 在将调用mapf将所有收集到的中间值保存到文件时，可以先在内存中创建nreduce个桶，然后再将这些值先映射入桶中，最后再将这些值存入中间文件。

### lab2A

1. 什么时候重置选举的计时器？  

   (1) 选举计时器时间到时，节点状态变为candidate，重置计时器  

   (2) 节点收到RequestVote的RPC请求时，若给予投票，则重置计时器  

   (3) 节点收到leader有效的heartbeat请求时(arg.Term > rf.term)              

2. 并发控制，对于互斥锁如何设计？   

   (1) 处理rpc方法时，可在一开始直接尝试换取锁，并紧接着使用defer释放锁   

   (2) 在调用rpc之前，要先释放锁，不然调用rpc请求可能会有较长延时，长时间占用锁不释放掉可不太好

   <img src="https://github.com/fravenx/oss/blob/master/img/%E6%88%AA%E5%B1%8F2023-09-07%2018.34.58.png" alt="截屏2023-09-07 18.34.58" style="zoom:40%;" />

3. 在该领导选举计时器方法中，若节点的状态为leader要退出时，记得用break语句而不是return语句，否则该节点的领导选举计时器会失效，且之后不再尝试成为leader

4. 节点收到发送的rpc请求回复时，要对比该请求发送时的args.Term是否和节点此时的Term相同，若不相同，该回复已过期。

### lab2B

&nbsp;&nbsp;&nbsp;<img src="https://github.com/fravenx/oss/blob/master/img/mit6.824/2B1.png"  style="zoom:40%;" />

1. log[]中的Entry会有一个唯一的index且之后不再更改，这个index和log[]切片的index不是相等的，而是一个映射的关系，我为了简单，写了一个二分的方式通过Entry.index找到其在log[]切片中位置。但由于go中运算符优先级有所调整，并不和c++一样，这里我原先没有加括号，导致一个节点死循环。这个错误我找了两三个小时才找到，一开始还以为是这个节点死锁，但是通过打日志发现这个节点在获得到互斥锁之后才停止操作，最终在这段临界区代码中定位到二分代码死循环的问题。

2. 节点在收到AppendEntry的RPC请求时，要对prevLogIndex和prevLogTerm进行检查，若不匹配，可以优化leader的nextIndex回退的方式。对此，我采取的优化策略是在RPC回复中增加一个FirstIndex的字段。

   (1) 若该follower节点中并不存在prevLogIndex对应的Entry，则置该FirstIndex = -1，leader收到回复后，将该follower对应的nextIndex置为prevLogTerm第一次出现的位置  

   (2) 若该follower节点中存在prevLogIndex对应的Entry，但该Entry.Term != prevLogTerm,则在FirstIndex填入follower的log中这个冲突Entry的Term第一次出现的位置，leader收到回复后，置该follower的nextIndex为firstIndex

   <img src="https://github.com/fravenx/oss/blob/master/img/mit6.824/2B2.png"  style="zoom:40%;" />

3. leader只会commit和自己的currentTerm相同term的Entry。因此，我的具体做法是在leader发送AppendEntry的RPC请求时，检查log[]中最后一条entry.term == rf.currentTerm为真时，才会给args.Entries[]字段添加leader中要尝试commit的Entry

4. leader发送AppendEntry的RPC请求收到reply.Success为真后，在更新matchIndex不能直接设置为Len(log[])，而是prevLogIndex + len(entries[])，因为在发送rpc请求后，leader中的log[]长度可能会改变

   <img src="https://github.com/fravenx/oss/blob/master/img/mit6.824/%E6%88%AA%E5%B1%8F2023-09-13%2002.28.32.png"  style="zoom:40%;" />

5. 遇到一个很微妙的bug，发生在系统刚启动时，观察日志发现节点S1刚刚选举计时器到达，紧接着4ms后收到节点S2的RequestVote的请求，按逻辑上应该选举计时器到达后，S1节点成为Candidate，增加自己Term，然后拒绝掉节点S2的RequestVote请求。但实际情况是由于我这里在选举计时器到达时先输出了一条日志，导致有点微小的延时，处理RequestVote的RPC请求逻辑先获得锁，并grant vote，节点S2变成leader，并在log[]中加了一条命令[101]，节点S1的term变成2，然后选举计时器逻辑获得锁，节点S1的term变为3，发送RequestVote请求，节点S2收到后，发现arg.term比自己大，于是变为follower，但由于日志完整性检查，并不会grant vote,由于网络中还有节点S0存在并给S1 grant vote，节点S1成为leader，但由于命令[101]在节点S2中，致使这条命令一直没有被commit,最后fail to reach agreement，这种情况比较少见，较小概率发生，我将选举计时器超时后输出日志的代码放入在获取互斥锁之后应该可以解决。这看似是可行，但是我又跑了2000遍测试出现一次失败，观察日志发现是上述情况没有4ms的延迟，S2的RequestVote请求到达S1和S1选举计时器超时同时发生，这种情况下只有S1选举计时器先抢占到锁才能成功通过测试，但是实际上S1选举计时器若没有抢占到锁，发生和上述同样的问题，难怪实验hint中说time.Timer比较tricky to use，所以最终实现改为time.Time记录上一次心跳时间，用当前时间来比对。

### lab2C







