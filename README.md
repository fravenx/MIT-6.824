## 实验结果

### lab1(10次)

<img src="https://github.com/fravenx/oss/blob/master/img/mit6.824/%E6%88%AA%E5%B1%8F2023-09-13%2016.14.53.png"  style="zoom:30%;" />

### 2A(2000次0fail)

<img src="https://github.com/fravenx/oss/blob/master/img/%E6%88%AA%E5%B1%8F2023-09-07%2018.24.32.png"  style="zoom:50%;" />

### 2B(10000次0fail)

<img src="https://github.com/fravenx/oss/blob/master/img/mit6.824/%E6%88%AA%E5%B1%8F2023-09-15%2013.38.30.png"  style="zoom:50%;" />

### 2C(10000次0fail)

<img src="https://github.com/fravenx/oss/blob/master/img/mit6.824/%E6%88%AA%E5%B1%8F2023-09-16%2000.20.43.png"  style="zoom:50%;" />

### 2D(10000次0fail)

<img src="https://github.com/fravenx/oss/blob/master/img/mit6.824/%E6%88%AA%E5%B1%8F2023-09-17%2019.34.39.png"  style="zoom:50%;" />

### 3A(500次0fail)

<img src="https://github.com/fravenx/oss/blob/master/img/mit6.824/3a1.png" style="zoom:50%;" />

### 3B(500次0fail)

<img src="https://github.com/fravenx/oss/blob/master/img/mit6.824/3b.png" style="zoom:50%;" />

### 4A(500次0fail)

<img src="https://github.com/fravenx/oss/blob/master/img/mit6.824/4a.png" style="zoom:50%;" />





## 过程中踩的坑/难点（除了严格遵守论文图2）
### lab1 
1. 我的临时文件最开始是放在src/mr目录下，在使用main程序时运行代码时当前目录为"src/main",程序能够正常运行。但在使用测试文件时，运行过程
中当前目录 为"src/main/mr-tmp"，会出现找不到目录的错误。
2. 在将调用mapf将所有收集到的中间值保存到文件时，可以先在内存中创建nreduce个桶，然后再将这些值先映射入桶中，最后再将这些值存入中间文件。

### 2A

1. 什么时候重置选举的计时器？  

   (1) 选举计时器时间到时，节点状态变为candidate，重置计时器  

   (2) 节点收到RequestVote的RPC请求时，若给予投票，则重置计时器  

   (3) 节点收到leader有效的heartbeat请求时              

2. 并发控制，对于互斥锁如何设计？   

   (1) 处理rpc方法时，可在一开始直接尝试换取锁，并紧接着使用defer释放锁   

   (2) 在调用rpc之前，要先释放锁，不然调用rpc请求可能会有较长延时，长时间占用锁不释放掉可不太好

3. 在该领导选举计时器方法中，若节点的状态为leader要退出时，记得用break语句而不是return语句，否则该节点的领导选举计时器会失效，且之后不再尝试成为leader🙂

   <img src="https://github.com/fravenx/oss/blob/master/img/%E6%88%AA%E5%B1%8F2023-09-07%2018.34.58.png" alt="截屏2023-09-07 18.34.58" style="zoom:40%;" />

4. 节点收到发送的rpc请求回复时，要对比该请求发送时的args.Term是否和节点此时的Term相同，若不相同，该回复已过期。

### 2B

1. 节点在收到AppendEntry的RPC请求时，要对prevLogIndex和prevLogTerm进行检查，若不匹配，可以优化leader的nextIndex回退的方式。对此，我采取的优化策略是在RPC回复中增加一个FirstIndex的字段。

   (1) 若该follower节点中并不存在prevLogIndex对应的Entry，则置该FirstIndex = -1，leader收到回复后，将该follower对应的nextIndex置为prevLogTerm第一次出现的位置  

   (2) 若该follower节点中存在prevLogIndex对应的Entry，但该Entry.Term != prevLogTerm,则在FirstIndex填入follower的log中这个冲突Entry的Term第一次出现的位置，leader收到回复后，置该follower的nextIndex为firstIndex

2. leader只会commit和自己的currentTerm相同term的Entry。因此，我的具体做法是在leader发送AppendEntry的RPC请求时，检查log[]中最后一条entry.term == rf.currentTerm为真时，才会给args.Entries[]字段添加leader中要尝试commit的Entry

   <img src="https://github.com/fravenx/oss/blob/master/img/mit6.824/2B2.png"  style="zoom:40%;" />

3. leader发送AppendEntry的RPC请求收到reply.Success为真后，在更新matchIndex不能直接设置为Len(log[])，而是prevLogIndex + len(entries[])，因为在发送rpc请求后，leader中的log[]长度可能会改变

4. 遇到一个很微妙的bug，发生在系统刚启动时，观察日志发现节点S1刚刚选举计时器到达，紧接着4ms后收到节点S2的RequestVote的请求，按逻辑上应该选举计时器到达后，S1节点成为Candidate，增加自己Term，然后拒绝掉节点S2的RequestVote请求。但实际情况是由于我这里在选举计时器到达时先输出了一条日志，导致有点微小的延时，处理RequestVote的RPC请求逻辑先获得锁，并grant vote，节点S2变成leader，并在log[]中加了一条命令[101]，节点S1的term变成2，然后选举计时器逻辑获得锁，节点S1的term变为3，发送RequestVote请求，节点S2收到后，发现arg.term比自己大，于是变为follower，但由于日志完整性检查，并不会grant vote,由于网络中还有节点S0存在并给S1 grant vote，节点S1成为leader，但由于命令[101]在节点S2中，致使这条命令一直没有被commit,最后fail to reach agreement，这种情况比较少见，较小概率发生，我将选举计时器超时后输出日志的代码放入在获取互斥锁之后应该可以解决。这看似是可行，但是我又跑了2000遍测试出现一次失败，观察日志发现是上述情况没有4ms的延迟，S2的RequestVote请求到达S1和S1选举计时器超时同时发生，这种情况下只有S1选举计时器先抢占到锁才能成功通过测试，但是实际上S1选举计时器若没有抢占到锁，发生和上述同样的问题，难怪实验hint中说time.Timer比较tricky to use，所以最终实现改为time.Time记录上一次心跳时间，用当前时间来比对。

   <img src="https://github.com/fravenx/oss/blob/master/img/mit6.824/%E6%88%AA%E5%B1%8F2023-09-13%2002.28.32.png"  style="zoom:40%;" />

### 2C

1. 2C就没什么好说的了，如果2A和2B设计的比较周到的话，2C就只需要在每次需要持久化的状态改变时调用下persist()函数就行，只不过2C的测试案例相比之前更加苛刻，增加了网络不稳点的情况。我运行10000次出现25次fail，错误内容为apply out of order，查看日志定位出问题在apply实现上，之前apply()都是在commitIndex更新后进行同步调用，违背了只能用一个实例来进行apply操作的原则，修改实现为节点启动后创建一个新的协程applier只用来进行apply操作，使用sync.Cond来同步，在lastApplied < commitIndex时用cond.Wait()来等待。

   

2. 又运行3000次出现了一次错误，情况如下：S0成为leader后添加命令在index187后，给S3的发送的心跳全部丢失，S1,S2,S4都与S0的log同步，S3选举计时器超时尝试成为leader，增加term发送RequestVote请求，S0收到请求后发现网络中有比自己term大的节点，转为follower，但由于日志完整检测，S0不会给S3投票，S0选举计时器超时之后又再次成为leader，但测试函数没有再给S0发送命令，S0中在index187的命令一直没有被复制到S3，最后整个测试运行了10分钟报超时错误，这种情况确实出乎意料，我的心跳是每100ms发一次，选举计时器是700-1000ms，S0给S3发送的至少7次心跳全部丢失，我的解决方法是增加candidate成为leader后立即发送一次心跳的机制，即可稳定通过10000次。

### 2D

1. 增加的InstallSnapshotRpc应该如何与图2交互？我的思考结果：

   (1) 在Snapshot(Index int, snapshot []byte)中，不必完全按照论文要求截断index之前的所有log（包括index），我的实现是截断index之前所有log（不包括index），这样明显实现更加简单，不然以前的一些代码要重新设计（AppendEntries的RPC请求对比上一条entry检查机制），边界情况也会增多。  

   (2)  什么时候发送InstallSnapshotRpc请求？在leader发送AppendEntriesRpc时出现nextIndex[i] <= rf.lastIncludedIndex情况时，因为nextIndex[i]是节点S[i]期待收到的下条命令乐观估计，rf.lastIncludedIndex是leader已经apply且其之前的log都已被删掉。

   (3)  节点在收到InstallSnapshot的Rpc请求时，若args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.commitIndex则该请求过期，直接丢弃。清空自己的log，写入一条index为args.LastIncludedIndex，term为args.LastIncludedTerm的Entry，command可以不填（这条entry只用于之后的日志添加一致性检查），并置lastapplied=commitIndex = args.LastIncludedIndex,向applych发送snapshot，以及持久化当前最新snapshot和raft_state

   <img src="https://github.com/fravenx/oss/blob/master/img/mit6.824/%E6%88%AA%E5%B1%8F2023-09-16%2023.16.15.png"  style="zoom:40%;" />

   (4)  leader收到InstallSnapshot请求回复时，在确保rpc发送期间leader状态未改变以及该回复未过期后，更新该节点的nextIndex和matchIndex分别为args.LastIncludedIndex + 1和args.LastIncludedIndex。

### 3A

1. 在server的RPC处理中，为了等待从appier中出来的命令响应Clerk而设置的chan，一定不要使用默认的无缓冲的chan，而要设置一个缓冲空间为1（一个client只会有一个命令at the moment），由于默认的chan是同步的，server中RPC请求和appier两者阻塞，系统会超时，命令一直无法完成 

   

2. client除了在Err为ErrWrongLeader需要更换leader，在调用RPC请求ok为false时也要更换leader，否则系统会超时，一直无响应。

   

3. client的clientId和seqno要写入Op结构体中，此操作是为了让follower的table能够同步更新，从而保证请求只会被执行一次。

### 3B

1. raftState大于阈值时持久化server的data和记录客户端最新序列号的table，以及sever启动时读取已保存的data和table。

2. 增加follower能够快速catch up的机制：

   1）在appendentrie的RPC请求中，若回复为false（preEntry检测策略失败），立即向该follower再次发送appendentries的RPC请求，而不要等到下一次心跳时间

   2）若leader更新了commitIndex，立即向其余follower发送心跳，以便follower也能够更新commitIndex

   3）在leader发送快照的请求中，此follower可能已经落后较多条日志，在发送完日志，收到肯定回复后，立即再次向该节点发送心跳。

   否则lab2能够稳定通过，而3B会超时。

### 4A

1. Rebalance函数的设计:

   用一个map记录gid到shard的映射，统计map中每个gid对应的shard数组的最大值和最小值，只要相差大于1时，不断移动最大shard数组中部分元素到最小shard数组中，并更新config.Shards，当gid为0的shard数组不为空时，优先清空gid为0的shard数组(gid非空)



