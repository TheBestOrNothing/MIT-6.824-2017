# MIT 6.824 2017
Thanks MIT PDOS group for your giving. 
----------------------------------------------------------------------------------------
The 6.824 open class is excellent as 6.828. It bring us(outside of MIT) into the world of distribute system. Your rasie me up more than I can do...

The following things have done:
  
  LAB1 - Map/Reduce input and output, Single-worker word count, Distributing MapReduce tasks, Handling worker failures, 
         Inverted index generation
  LAB2 - leader election and heartbeats, Appending log entries, Persist raft states, Figure8.
  LAB3 - Key/value service(Data Base), KV client, Snapshot log compaction.
  LAB4 - No implement.
  
The following issues still have:
  1. The probability of passing Figure8(unreliable) is lager than 80%, Because of the unreliable network 
     which make follower start    election frequently and have little time to find correct prevIndex.
  2. There will be miss element occasionally when run TestManyPartitionsManyClients testcase in lab3.
     wanted: x 0 0 yx 0 1 yx 0 2 y
     got   : x 0 0 yx 0 1 y
    
What is my next step?
  I've warmed up when pass the test lab by lab. Now let us start bitcoin journey. 

How to contact with me?
  gongzhe@live.com
