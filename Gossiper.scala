import akka.actor._
import util.control.Breaks._
import com.typesafe.config.ConfigFactory
import scala.collection._
import akka.actor.Props
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit;


// NODES GOSSIP EVERY 10ms INTERVAL. THIS IS HAS BEEN ACHIEVED BY USING AKKA SCHEDULER INSIDE EVERY ACTOR.



case class PushSum(s: Double, w: Double)

object RandomGen {
  var klass = scala.util.Random
}

object PrintFlag {
  var e = false
}

object Settings {
  var algos = Array("gossip", "push-sum")
  var topos = Array("full", "line", "3d", "imp3d")
}

object RunTime {
  var start = System.currentTimeMillis()
}

object Nodes {
  var arr   = new Array[ActorRef](0) // full, line
  var arr_3d = Array.ofDim[ActorRef](0,0,0) // 3d, imp3d
  var arr_3d_values = Array.ofDim[Int](0,0,0) // 3d to 1d mapping
  var arr_imp3d_othernode = Array.ofDim[String](0,0,0) // other node's coordinate string
  var arr_3d_indices = mutable.MutableList[String]()
  var converged = 0
  
  var count = 0 // no. of nodes in the network
  var topology = "": String
  var algorithm = "": String
  
  def selectNeighbourFull(nodeNum: String): ActorRef = {
    var neighNodeNum = -1
    var currNodeNum = nodeNum.toInt
    do {
      neighNodeNum = RandomGen.klass.nextInt(Nodes.count)
    } while (neighNodeNum == currNodeNum) // Node should not rumor itself!
        
    return Nodes.arr(neighNodeNum)    
  }
  
  def selectNeighbourLine(nodeNum: String): ActorRef = {
    var neighNodeNum = -1
    var currNodeNum = nodeNum.toInt
    if(currNodeNum == 0){ // select right neighbour
      neighNodeNum = 1
          
    } else if(currNodeNum == (Nodes.count - 1)) { // select left neighbour
      neighNodeNum = Nodes.count - 2
          
    } else { // select either l or r neighbour         
      var direction = RandomGen.klass.nextInt(2)
      if(direction == 0){
        neighNodeNum = currNodeNum - 1
      }else{
        neighNodeNum = currNodeNum + 1
      }          
    }
        
    return Nodes.arr(neighNodeNum)    
  }
  
  
  def neighbours3d(nodeNum: String): Set[String] = {
        var nodeCoords = nodeNum.split(",")
        var min = 0
        var max = Nodes.arr_3d.length - 1
        
        var x = nodeCoords(0).toInt
        var y = nodeCoords(1).toInt
        var z = nodeCoords(2).toInt
        
        var neighbourCoords = mutable.MutableList[String]()
       
        if(x == min){ // cant go further left
          neighbourCoords += ((x+1).toString + "," + y.toString + "," + z.toString)  
          
        } else if(x == max) { // cant go further right
          neighbourCoords += ((x-1).toString + "," + y.toString + "," + z.toString)   
          
        } else {
          neighbourCoords += ((x+1).toString + "," + y.toString + "," + z.toString)
          neighbourCoords += ((x-1).toString + "," + y.toString + "," + z.toString)
        }
        
        
        if(y == min){ // cant go further down
          neighbourCoords += (x.toString + "," + (y+1).toString + "," + z.toString) 
          
        } else if(y == max) { // cant go further up
          neighbourCoords += (x.toString + "," + (y-1).toString + "," + z.toString)
          
        } else {
          neighbourCoords += (x.toString + "," + (y+1).toString + "," + z.toString)
          neighbourCoords += (x.toString + "," + (y-1).toString + "," + z.toString)
        }
        

        if(z == min){ // cant go further back
          neighbourCoords += (x.toString + "," + y.toString + "," + (z+1).toString)
          
        } else if(z == max) { // cant go further front
          neighbourCoords += (x.toString + "," + y.toString + "," + (z-1).toString)
          
        } else {
          neighbourCoords += (x.toString + "," + y.toString + "," + (z+1).toString)
          neighbourCoords += (x.toString + "," + y.toString + "," + (z-1).toString)
        }
        
        var neighbourCoordsSet =  collection.mutable.Set(neighbourCoords.toSet.toArray:_*)
        
        // loop thru the set and remove null nodes
        var n_arr = new Array[String](0)
        var a,b,c = 0
        for(n <- neighbourCoordsSet) {
           n_arr = n.split(",")
           a = n_arr(0).toInt
           b = n_arr(1).toInt
           c = n_arr(2).toInt
           
           if(Nodes.arr_3d(a)(b)(c) == null) { // no node at this index, delete index from set
             neighbourCoordsSet.remove(n)
           }          
        } 
        
        return neighbourCoordsSet
  }
  

  def randomFromNeighbours(neighbourCoordsSet: Set[String]): ActorRef = {
    var randNeighbour = neighbourCoordsSet.toVector(RandomGen.klass.nextInt(neighbourCoordsSet.size))        
    var randNeighbourCoords = randNeighbour.split(",")
        
    var x = randNeighbourCoords(0).toInt
    var y = randNeighbourCoords(1).toInt
    var z = randNeighbourCoords(2).toInt
        
    return Nodes.arr_3d(x)(y)(z)     
  }
  
  // for both 3d and imp3d
  def selectNeighbour3d(nodeNum: String): ActorRef = {
    var neighbsWithNodes = neighbours3d(nodeNum)
    var neighbourCoordsSet = collection.mutable.Set(neighbsWithNodes.toArray:_*)
    
    if(topology.contains("imp3d")){
      var nodeCoords = nodeNum.split(",")
      var x = nodeCoords(0).toInt
      var y = nodeCoords(1).toInt
      var z = nodeCoords(2).toInt
        
      var otherCoord   = Nodes.arr_imp3d_othernode(x)(y)(z)
      var currCoordSet = collection.mutable.Set(nodeNum)
      
      if(otherCoord == null) {
        var allCoordsSet   = collection.mutable.Set(Nodes.arr_3d_indices.toSet.toArray:_*)
        var otherCoordsSet = (allCoordsSet -- neighbourCoordsSet) -- currCoordSet // non neighbours
        
        if(otherCoordsSet.size > 0) {
          otherCoord = otherCoordsSet.toVector(RandomGen.klass.nextInt(otherCoordsSet.size))
          Nodes.arr_imp3d_othernode(x)(y)(z) = otherCoord
        }
      }
      
      neighbourCoordsSet += otherCoord
    }
        
    return randomFromNeighbours(neighbourCoordsSet)
  }
  
  def selectNeighbour(nodeNum: String, senderNode: ActorRef): ActorRef = {
    var num = -1 // initialize with some value
    
    topology match {
      case "full" => return selectNeighbourFull(nodeNum)
      case "line" => return selectNeighbourLine(nodeNum)        
      case "3d"   => return selectNeighbour3d(nodeNum)
      case "imp3d" => return selectNeighbour3d(nodeNum)
    }    
  }
  
} // object Nodes

// nodes that have heard the rumor at least once
object NodesWithRumor {
  var arr   = mutable.MutableList[ActorRef]()
  var count = 0
  var boredCount = 0 // nodes who have heard rumor 10 times
}

class NodeActor extends Actor {
  import context._

  val rumorHeardMax = 50
  var rumorHeardCount = 0
  var s = sInit()
  var w = 1.toDouble
  var ratio = ratioInit()
  var conseqCount = 0
  var ratioDiffLimit = Math.pow(10, -10)
  val nodeName = self.path.name
  var cancellable: Cancellable = null
  
  def sInit(): Double = {
    var s_temp = 0
    var nodeNum = self.path.name.split("NodeActor")(1)
    if(Nodes.topology.contains("3d")) {
      var nodeCoords = nodeNum.split(",")
        
      var x = nodeCoords(0).toInt
      var y = nodeCoords(1).toInt
      var z = nodeCoords(2).toInt
      
      s_temp = Nodes.arr_3d_values(x)(y)(z)
    } else {     
      s_temp = nodeNum.toInt + 1
    }
    
    return s_temp.toDouble
  }  
  
  def ratioInit(): Double = {
    return (s/w)
  }
  
  // The actors sends rumors unless it has received 10 rumors itself
  def sendRumor = {
    if (rumorHeardCount < rumorHeardMax) {
      var nodeNumber = nodeName.split("NodeActor")(1) // format of the number, eg: 3 (1d) or 101 (3d)
      var node = Nodes.selectNeighbour(nodeNumber, sender)  // Topologies handled here
  
//      println("[Uniq:" + NodesWithRumor.count + "][HeardCount:" + rumorHeardCount + "]" + nodeName + " -> " + node.path.name)
      node ! "rumor"
      
    } else {   
      if(rumorHeardCount == rumorHeardMax) {
        NodesWithRumor.boredCount = NodesWithRumor.boredCount + 1        
        cancellable.cancel()
      }
            
      // select any other node with the rumor to propagate the rumor
      if(allNodesRumored || Nodes.converged==1) {
//        println("CANT SEND RUMORS. SYSTEM CONVERGED!")
        wrapUpGossiping 

      } else {
        if(NodesWithRumor.boredCount == NodesWithRumor.count) {
//          println("ALL NODES ARE BORED. System has no nodes left that want to rumor.")
          wrapUpGossiping  
        } else {
      
        } 
      }      
    }
  }
  
  
  def handleRumor = {    
    if(allNodesRumored || Nodes.converged==1) {
      if(PrintFlag.e == false) {
        println("[" + nodeName +"] " +" PROGRAM TIMESTAMP WHEN ACTOR STOPPED = " + (System.currentTimeMillis - RunTime.start).toString + " ms")
        PrintFlag.e = true
      }
      
      wrapUpGossiping
      
    } else {      
      hearAndSchedule("StartGossip")
      sendRumor     
    }
  }
  
  def wrapUpGossiping = {
      Nodes.converged = 1
      shutdown    
  }
  
  def allNodesRumored(): Boolean = {
    return (NodesWithRumor.count >= Nodes.count) 
  }
  
  def sendPushSum = {
    if(pushSumConverged) { // node has converged
      ratioLimitReached
      
    } else {

      var s_push = (s/2)
      var w_push = (w/2)
      
      // update local data
      s     = (s/2)
      w     = (w/2)
      ratio = (s/w)
        
      // select a neighbour node and pushSum
      var nodeNumber = nodeName.split("NodeActor")(1) // format of the number, eg: 3 (1d) or 101 (3d)
      var node = Nodes.selectNeighbour(nodeNumber, sender)  // Topologies handled here
//      println("[Uniq:" + NodesWithRumor.count + "]" + "[" + conseqCount + "]" + nodeName + " -> " + node.path.name + ". S_PUSH = " + s_push + ", W_PUSH = " + w_push + ", RATIO_CURR = " + ratio)
      node ! PushSum(s_push, w_push)
    
    }
  }
  
  def handlePushSum(s_push: Double, w_push: Double) = {
    if(pushSumConverged) {
      ratioLimitReached
      
    } else { 
      var ratio_old = (s/w)
      
      // update local data
      s     = s + s_push
      w     = w + w_push
      ratio = (s/w)
            
      // update conseqCount
      var ratioDiff = Math.abs(ratio - ratio_old)
      if(ratioDiff < ratioDiffLimit) { // node has encountered less then 10^-10 difference
        conseqCount = conseqCount + 1
      } else {
        conseqCount = 0
      }

      if(conseqCount == 3) { // Node Converged, and system converged
      println("TIME TAKEN = " + (System.currentTimeMillis - RunTime.start).toString + " ms")
        println(System.currentTimeMillis - RunTime.start)
        ratioLimitReached 
      } else {
        sendPushSum
      }

      hearAndSchedule("SendSum")
      
    }    
  }
  
  def hearAndSchedule(msg: String) = {
    rumorHeardCount = rumorHeardCount + 1  
    if (rumorHeardCount == 1) {
      NodesWithRumor.count = NodesWithRumor.count + 1        
      NodesWithRumor.arr += self //add current node to the list of nodes who have heard the rumor
      if(Nodes.algorithm == "push-sum"){
      }else{
        cancellable = system.scheduler.schedule(Duration.Zero, Duration.create(10, TimeUnit.MILLISECONDS), self, msg)
      }
    }    
  }
  
  def pushSumConverged(): Boolean = {
    return (conseqCount >= 3 || Nodes.converged == 1)
  }
  
  def ratioLimitReached = {
    Nodes.converged = 1
    shutdown
  }
  
  def shutdown = {
    if(Nodes.algorithm == "push-sum"){   
    }else{
      cancellable.cancel() 
    }
    context.system.shutdown()
  }
    
  def receive = {
    case "rumor"       => handleRumor
    case "StartGossip" => sendRumor
    case "SendSum"     => sendPushSum
    case PushSum(s_push, w_push) => handlePushSum(s_push, w_push)
    case msg: String   => println(s"RemoteActor received message '$msg'")
  }
}

object Gossiper extends App {
  var numNodes = args(0).toInt
  Nodes.topology = args(1).toLowerCase
  Nodes.algorithm = args(2).toLowerCase
  Nodes.count = numNodes
  
  if(!(Settings.topos.contains(Nodes.topology)) || (!Settings.algos.contains(Nodes.algorithm))) {
    println("\nInvalid parameters!")
    println("\nPlease Use:\nTopologies -> " + Settings.topos.deep.mkString(", ") + "\nAlgorithms -> " + Settings.algos.deep.mkString(", "))
    System.exit(1)
  }
  
  
  var msg = ""
  if(Nodes.algorithm == "push-sum"){
    msg = "SendSum"
  } else {
    msg = "rumor"
  }
  
  val actorSystem = ActorSystem("GossiperSystem", ConfigFactory.parseString(get_config))
  
  if(Nodes.topology.contains("3d")) {
    var size = Math.ceil(Math.cbrt(numNodes)).toInt
    
    Nodes.arr_3d = Array.ofDim[ActorRef](size,size,size)
    Nodes.arr_3d_values = Array.ofDim[Int](size,size,size)
    Nodes.arr_imp3d_othernode = Array.ofDim[String](size,size,size)
    
    var l = 0
    var nodeIndex = ""
    
    breakable {
      for(i <- 0 until size) {
        for(j <- 0 until size) {
          for(k <- 0 until size) {
            nodeIndex = i.toString + "," + j.toString + "," + k.toString
            Nodes.arr_3d(i)(j)(k) = actorSystem.actorOf(Props[NodeActor], name = "NodeActor" + nodeIndex) 
            Nodes.arr_3d_values(i)(j)(k) = (l+1).toInt
            Nodes.arr_3d_indices += nodeIndex
            
            l = l + 1
            if (l == Nodes.count) {
              break
            } 
          }
        }
      }
    }
        
    RunTime.start = System.currentTimeMillis()
    Nodes.arr_3d(0)(0)(0) ! msg
         
  } else {
    Nodes.arr = new Array[ActorRef](numNodes)
    for(i <- 0 until Nodes.count) {
      Nodes.arr(i) = actorSystem.actorOf(Props[NodeActor], name = "NodeActor" + i.toString)
    }
    
    // start a node actor
    RunTime.start = System.currentTimeMillis()
    Nodes.arr(0) ! msg
    
    sys.addShutdownHook(println("DONEEEE"))
    
  }
  
  def get_config() : String = {
      var config = """
      akka {
      loglevel = "OFF"
      log-sent-messages = off
      log-received-messages = off
      }""" : String
      
      return config
  }
  
}




