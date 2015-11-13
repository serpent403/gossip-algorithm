How to run:
$sbt "run numNodes topology algorithm"
Note: numNodes refers to the number of desired nodes in topology.
topology refers to one of the options: full, line, 3d, imp3d
algorithm refers to one of the options: gossip, push-sum

What is working:
Gossip Algorithm implementated for all the four topologies(Full, Line, 3D, Imperfect 3D)
Push-Sum Algorithm implemented for all the four topologies(Full, Line, 3D, Imperfect 3D)

NODES GOSSIP EVERY 10ms INTERVAL. THIS HAS BEEN ACHIEVED BY USING AKKA SCHEDULER INSIDE EVERY ACTOR.
Maximum CPU utilized is 375%

The largest network we managed to deal with for each type of
topology and algorithm is
Gossip:
Full - 8192 Nodes
Line - 8192 Nodes
3D - 8192 Nodes
Imperfect 3D - 8192 Nodes

Push-Sum
Full - 8192 Nodes
Line - 8192 Nodes
3D - 8192 Nodes
Imperfect 3D - 8192 Nodes
