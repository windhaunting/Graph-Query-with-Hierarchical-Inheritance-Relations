/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

// bfs reference: https://gist.github.com/ankurdave/63acd24ef744aaac87e0

package main.scala.hierarchialQuery

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

import collection.immutable.{Map,HashMap}
import scala.collection.mutable.ListBuffer

import main.scala.hierarchialQuery.nodeTypeEnum._
import main.scala.hierarchialQuery.visitedColorEnum._

import scala.reflect.ClassTag
import scala.util.control.Breaks._
import scala.util.control.Breaks._

import java.io._

object starQuery {
 
  var TOPK = 0                 //top K candidate answer, set by main function parameter
  val ALPHA = 0.9               //propagation factor
  val N = 1.05
  val numTasks = 8                   //how many task for one core can execute in parallell

  var topKKthLowerBoundScore = 0.0        // the smallest (kth) lowest upper bound score in the k list
  
  //one source bfs from one src to dst
  def bfs[VD, ED](graph: Graph[VD, ED], src: VertexId, dst: VertexId): Seq[VertexId] = {
    if (src == dst) return List(src)

    // The attribute of each vertex is (dist from src, id of vertex with dist-1 , parent node)
    var g: Graph[(Int, VertexId), ED] =
      graph.mapVertices((id, _) => (if (id == src) 0 else Int.MaxValue, 0L)).cache()
     
    
    //test
    g.vertices.take(10).foreach(println)
    // Traverse forward from src
    var dstAttr = (Int.MaxValue, 0L)
    while (dstAttr._1 == Int.MaxValue) {
      val msgs = g.aggregateMessages[(Int, VertexId)](
        e => if (e.srcAttr._1 != Int.MaxValue && e.srcAttr._1 + 1 < e.dstAttr._1) {
          e.sendToDst((e.srcAttr._1 + 1, e.srcId))
        },
        (a, b) => if (a._1 < b._1) a else b).cache()

      if (msgs.count == 0) return List.empty
      msgs.take(5).foreach(println)

      g = g.ops.joinVertices(msgs) {
        (id, oldAttr, newAttr) =>
          if (newAttr._1 < oldAttr._1) newAttr else oldAttr
      }.cache()
      
      dstAttr = g.vertices.filter(_._1 == dst).first()._2
      
     // println("bfs dstAttr: ", dstAttr)
     // g.vertices.take(5).foreach(println)

    }

    // Traverse backward from dst and collect the path
    var path: List[VertexId] = dstAttr._2 :: dst :: Nil
    //println("bfs 50 initial Path: ", path)
    
    while (path.head != src) {
      path = g.vertices.filter(_._1 == path.head).first()._2._2 :: path
    }

     //println("sssp path:  ", path)

    path
 }


 /*
//single specific node query
  def singleSourceGraphbfsTraverse[VD, ED](graph: Graph[VD, ED], specificNodeId: VertexId, dstTypeId: Int) = {
  
   // if (specificNodeId == dst) return List(specificNodeId)

    // The attribute of each vertex is (VD (nodetypeId), NodeInfo )
    var g: Graph[(VD, NodeInfo), ED] =
      graph.mapVertices((id, nodeIdType) => (nodeIdType, NodeInfo(specificNodeId, if (id == specificNodeId) 0 else Long.MaxValue, if (id == specificNodeId) 1 else 0, 0, 0.0, 0))).cache()
    
    //g.vertices.take(10).foreach(println)
    //println("77 singleSourceGraphbfsTraverse :  ")

    val tt01Attr = g.vertices.filter(_._1 == 1)
    tt01Attr.collect().foreach(println)
      
    //var dstAttr = ()
    var iterationCount = 0             //traverseCount <= topK
    var currentCandidateNumber: Long = 0
    while (currentCandidateNumber < TOPK)        // find top k or whole graph iteration end
    {
      
      //equal to create a new rdd
      val msgs: VertexRDD[(VD, NodeInfo)] = g.aggregateMessages[(VD, NodeInfo)](
        triplet => {
           //println("108 singleSourceGraphbfsTraverse: ", triplet.srcAttr._2)
          if (triplet.srcAttr._2.spDistance != Long.MaxValue && triplet.srcAttr._2.spDistance + 1  < triplet.dstAttr._2.spDistance)
          {
              
              val specificNodeIdType = graph.vertices.filter(x=>x._1 == specificNodeId).first()._2.toString.toInt       //specific node type is vlunerablity 
              if (getHierarchicalInheritance(specificNodeIdType, dstTypeId)){
                //update spDist and parentId, and hierachical level distance
                val changedEdgeLevel: Int = triplet.attr.toString.toInt
                val tmpNodeInfo = triplet.srcAttr._2.copy(spDistance = triplet.srcAttr._2.spDistance+1,
                                   hierLevelDifference = triplet.srcAttr._2.hierLevelDifference + changedEdgeLevel, parentId = triplet.srcId)  
                val currentNodeTypeId = triplet.dstAttr._1  
                triplet.sendToDst((currentNodeTypeId, tmpNodeInfo))

              }
              else{
                //update spDist and parentId
                val tmpNodeInfo = triplet.srcAttr._2.copy(spDistance = triplet.srcAttr._2.spDistance+1, parentId = triplet.srcId) 
                  val currentNodeTypeId = triplet.dstAttr._1  
                  triplet.sendToDst((currentNodeTypeId, tmpNodeInfo))
              }

            //  println("128 singleSourceGraphbfsTraverse: ", triplet.srcAttr._2)
          }
        },
        
        (a, b) => {
          if (a._2.spDistance < b._2.spDistance){
            a
          }
          else if(a._2.spDistance == b._2.spDistance){
            val tmpNodeInfo = a._2.copy(spNumber = a._2.spNumber+1)  //update spNumber
            val nodeTypeId = a._1
            (nodeTypeId, tmpNodeInfo)

          }
          else{
             b
          }
        }
      ).cache()
      
      //if (msgs.count == 0) return List.empty

      g = g.ops.joinVertices(msgs) {
        (nodeId, oldAttr, newAttr) =>
        if (newAttr._2.spDistance < oldAttr._2.spDistance) 
        {
          val spDistance = newAttr._2.spDistance
          val spNumber = newAttr._2.spNumber
          val hierLevelDifference =  newAttr._2.hierLevelDifference*(1)          //-1*hierLevelDifference； downward inheritance
          val newClosenessScore = calculateClosenessScore(spDistance, spNumber, hierLevelDifference)
          
          val tmpNodeInfo = newAttr._2.copy(closenessNodeScore = newClosenessScore)  //update closenessNodeScore 
          val nodeTypeId = newAttr._1
          //print ("157: singleSourceGraphbfsTraverse: ", nodeId, spDistance, spNumber, hierLevelDifference, newClosenessScore)
          (nodeTypeId, tmpNodeInfo)
        }
        else{
          oldAttr
        }        
      }.cache()
      
    
    //check candidate nodes   //&& _._2._2.spDistance != Long.MaxValue
    currentCandidateNumber = g.vertices.filter(x=>(x._2._1 == dstTypeId && x._2._2.spDistance != Long.MaxValue)).count()
    
    //check whether all nodes have been traversed, iteration ends
    /*
    val test01Attr = g.vertices.filter(_._1 == 1)
    test01Attr.collect().foreach(println)

    val test02Attr = g.vertices.filter(_._1 == 2)
    test02Attr.collect().foreach(println)
    */
    println("136 singleSourceGraphbfsTraverse g vertices: ", currentCandidateNumber)

    //g.vertices.take(6).foreach(println)
    iterationCount = iterationCount + 1
  }
  
  val candidateNodesRdd = g.vertices.filter(x=>(x._2._1 == dstTypeId && x._2._2.spDistance != Long.MaxValue))

  candidateNodesRdd.collect().foreach(println)
  
  candidateNodesRdd
}
*/
  //given the node types, hierarchical inheritance or not (hierarchical or generic relations)
  def getHierarchicalInheritance(nodeIdType1: Int, nodeIdType2: Int) = {
    //print ("189 getHierarchicalInheritance PRODUCT.id: ", PRODUCT.id +" " + VULNERABILITY.id)
    if ((nodeIdType1 == PRODUCT.id && nodeIdType2 == VULNERABILITY.id) || (nodeIdType1 == VULNERABILITY.id && nodeIdType2 == PRODUCT.id))
    {
      true
    }
    else{
      false
    } 
  }
  

//get the closeness score from the parameters
  def calculateClosenessScore(spDistance: Long, spNumber: Long, hierLevelDifference: Long) = {
    
    val closeScore: Double = math.min(N*scala.math.pow(ALPHA, (spDistance-hierLevelDifference)), spNumber*scala.math.pow(ALPHA, (spDistance -hierLevelDifference).toDouble))                         //just use math.pow( ) deprecated? http://alvinalexander.com/scala/scala-math-power-exponent-exponentiation-function
    closeScore
  }

  
  //calculate node score given all specific nodes in nodeMap
  def calculateNodeScoreStarquery(nodeMap: Map[VertexId, NodeInfo]) = {
    val nodeNum: Double = nodeMap.size
    var sumClosenessScore: Double = 0.0
    for ((specNodeId, nodeInfo) <- nodeMap){
      sumClosenessScore += nodeInfo.closenessNodeScore
    }
    sumClosenessScore/nodeNum
  }
  
  
    
         
  //get the shortest path of visited nodes from specic node
  def getPathforAnswers[VD, ED](sc: SparkContext, topkResultRdd: RDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))], graphExtended: Graph[(VD, Map[VertexId, NodeInfo]), ED] ) = {
    var pathAnswersLst = new ListBuffer[(VertexId, Map[VertexId, ListBuffer[VertexId]])]()
    val topkResultList = topkResultRdd.collect().toList
    //print ("227 getPathforAnswers ttttttttt: " + topkResultList)
    
    for (lstTuple <- topkResultList){
      val nodeId = lstTuple._1
      val nodeMap = lstTuple._2._5           //
      
      //defin getVisitedNodesPathh function
      def getVisitedNodesPath[VD, ED](nodeId: VertexId, nodeMap: Map[VertexId, NodeInfo], graph: Graph[(VD, Map[VertexId, NodeInfo]), ED]) = {
          var pathLstMap = Map[VertexId, ListBuffer[VertexId]]()        //key is the specific node, List is the shortest path from specific node to the dstnode
          //get every specific node's different path
          for ((specific, nodeInfo) <- nodeMap){
            //var tmpPathLst: List[VertexId] = nodeId :: Nil
            var tmpPathLstBuffer = new ListBuffer[VertexId]()
            tmpPathLstBuffer += nodeId
            //print ("220 getPathforAnswers: " + specific + " " +  tmpPathLstBuffer.head)
            // while(tmpPathLst.head != specific){
            //  tmpPathLst = graphExtended.vertices.filter(_._1 == tmpPathLst.head).first()._2._2(specific).parentId :: tmpPathLst         //  iterate to add prarent node
           //}

               while (tmpPathLstBuffer.head != specific)
               {
                 
                 var prevNodeId: Long = -888888
                 if (graph.vertices.filter(_._1 == tmpPathLstBuffer.head).count != 0){
                   prevNodeId = graph.vertices.filter(_._1 == tmpPathLstBuffer.head).first()._2._2(specific).parentId
                 }
                 else{
                   
                    tmpPathLstBuffer.insert(0, specific)

                 }
                 //print ("262 getPathforAnswers pathLstMap: " +  prevNodeId + " " + tmpPathLstBuffer.head)

                 //tmpPathLstBuffer.insert(0, prevNodeId)          //  iterate to add prarent node
                 tmpPathLstBuffer.insert(0, prevNodeId)

               }

           // pathLstMap += (specific->tmpPathLst)  
           // tmpPathLst = List()            //clear tmpPathLst
            pathLstMap += (specific->tmpPathLstBuffer)  
           // tmpPathLstBuffer.clear() 
        }
        pathLstMap
   } 
   
    val pathLstMap = getVisitedNodesPath(nodeId, nodeMap, graphExtended)
    pathAnswersLst += ((nodeId, pathLstMap))
  }
    
  val pathAnswersRDD = sc.parallelize(pathAnswersLst)
  //println("270 starQueryGraphbfsTraverse pathAnswersLst :" + pathAnswersLst +"\n")
  pathAnswersRDD
  
}
 
  

//inner local update lower bound closeness score
def calculateLowerBound[VD, ED](specificNodeId: VertexId, nodeMap: Map[VertexId, NodeInfo]) = {
  
    val prevIterLowerScore = nodeMap(specificNodeId).lowerBoundCloseScore
    var updatedLowerBoundCloseScore = 0.0
    if (prevIterLowerScore > 0.0)
    {
        updatedLowerBoundCloseScore = nodeMap(specificNodeId).lowerBoundCloseScore
    }
    else
    {
        //aggregate from A's lower bound close score
        updatedLowerBoundCloseScore = ALPHA * nodeMap(specificNodeId).lowerBoundCloseScore
    }
    updatedLowerBoundCloseScore
    
}

//global update lower bound closeness score
def calculateUpperBound(currentLowerBound: Double, iterationNumber: Long, hierLevelDifference: Long) = {
   
    var updatedUpperBoundCloseScore = 0.0
    if (currentLowerBound > 0)
      {
        updatedUpperBoundCloseScore = currentLowerBound
      }
    else
      {
        updatedUpperBoundCloseScore = scala.math.pow(ALPHA, (iterationNumber-hierLevelDifference))
      }
    updatedUpperBoundCloseScore    
    
}


//get matching Score lowerBound from nodeMap
def calculateMatchingScoreLowerBound(nodeMap: Map[VertexId, NodeInfo]) = {
    val nodeNum: Double = nodeMap.size
    var matchingScoreLowerBound: Double = 0.0

    for ((specNodeId, nodeInfo) <- nodeMap){
      matchingScoreLowerBound += nodeInfo.lowerBoundCloseScore
    }
    matchingScoreLowerBound/nodeNum
  }
  
//get matching Score upper Bound from nodeMap
def calculateMatchingScoreUpperBound(nodeMap: Map[VertexId, NodeInfo]) = {
    val nodeNum: Double = nodeMap.size
    var matchingScoreUpperBound: Double = 0.0

    for ((specNodeId, nodeInfo) <- nodeMap){
      matchingScoreUpperBound += nodeInfo.upperBoundCloseScore
    }
    matchingScoreUpperBound/nodeNum
  }
  
  
 //get all visited nodes' matching lower and upper bound score
def setnodeIdColorForBound[VD, ED](allNodesVisited: VertexRDD[(VD, Map[VertexId, NodeInfo])], g: Graph[(VD, Map[VertexId, NodeInfo]), ED]) = {
  
    val updatedNodeRdd = allNodesVisited.map{
      case (nodeId, (nodeIdType, nodeMap))=>
        val matchingScoreUpperBound = calculateMatchingScoreUpperBound(nodeMap)
        
        if (matchingScoreUpperBound < topKKthLowerBoundScore)          //upperbound less than the kth lowerbound score
        {
          var newMap = Map[VertexId, NodeInfo]()

          for ((specNodeId, nodeInfo) <- nodeMap){
            val tmpNodeInfo = nodeMap(specNodeId).copy(visitedColor = RED.id)  //update color visited
            newMap += (specNodeId -> tmpNodeInfo)
          }
          
          (nodeId, (nodeIdType, newMap))       
        }
        else
        {
          (nodeId, (nodeIdType, nodeMap))       
        }
        
    }
    
  val newG = g.ops.joinVertices(updatedNodeRdd) {
        (nodeId, oldAttr, newAttr) => 
         
        newAttr
        
  }.cache()                      //cache or not
 
     // println("372 xxxxxxx node vertices edgecount: ", g.vertices.count, newG.vertices.count)

  newG
  
}


//star query traverse with pruning
// starQueryGraphbfsTraverseWithBoundPruning
def starQueryGraphbfsTraverseWithBoundPruning[VD, ED](sc: SparkContext, graph: Graph[VD, ED], specificNodeIdLst: List[(VertexId, Int)], dstTypeId: Int, runTimeoutputFilePath: String) = {
     
    val startTime = System.currentTimeMillis()              //System.nanoTime()

    //Vertex's  property is (VD, Map[VertexId, NodeInfo]) ; VD is the node property-- nodeIdType here; Map's key: specificNodeId, value is NodeInfo
    var g: Graph[(VD, Map[VertexId, NodeInfo]), ED] =
      graph.mapVertices((id, nodeIdType) => (nodeIdType, 
                                             specificNodeIdLst.map(specificNodeIdType=> specificNodeIdType._1-> NodeInfo(specificNodeIdType._1, 
                                                                                                                         specificNodeIdType._2, if (id == specificNodeIdType._1) 0 else Long.MaxValue, if (id == specificNodeIdType._1) 1 else 0, 0, 0.0, 0, if (id == specificNodeIdType._1) GREY.id else WHITE.id, if (id == specificNodeIdType._1) 1.0 else 0.0,  if (id == specificNodeIdType._1) 1 else {N*scala.math.pow(ALPHA, 1)})).toMap
      )).cache()
                                 
    // g.vertices.take(5).foreach(println)
    //println("486 starQueryGraphbfsTraverseWithBoundPruning :  \n")
    //iterations for bfs begin
    var iterationCount = 0
    var currentSatisfiedNodesNumber: Long = 0L                   //number of dest type nodes that visited
    var allNodesVisitedNumber: Long = 0L                         // all nodes visited
    var oldAllNodesVisitedNumber: Long = -1L                     //previous iteration nodes visited
    var twoPreviousOldAllNodesVisitedNumber: Long = -2L          //previous and previous iteration nodes visited
    
    var topKResultRdd: RDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))] = sc.emptyRDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))]           //Return Result RDD, (nodeId, matchingScore, lowerBound, upperBound, nodeMap)

    
    var pathAnswerRdd: RDD[(VertexId, Map[VertexId, ListBuffer[VertexId]])]  = sc.emptyRDD[(VertexId, Map[VertexId, ListBuffer[VertexId]])]           //Return Result RDD, (nodeId, matchingScore, lowerBound, upperBound, nodeMap)

    val dstNodesNumberGraph = graph.vertices.filter(x=>x._2 == dstTypeId)
    //println("504 starQueryGraphbfsTraverseWithBoundPruning :  ", dstNodesNumberGraph.count)
   
  //no value change or all the destination node has been visited
  while (twoPreviousOldAllNodesVisitedNumber != oldAllNodesVisitedNumber && currentSatisfiedNodesNumber < dstNodesNumberGraph.count && allNodesVisitedNumber < graph.ops.numVertices) //currentSatisfiedNodesNumber < TOPK &&; find top k or whole graph iteration end    {
  //while (currentSatisfiedNodesNumber < dstNodesNumberGraph.count && allNodesVisitedNumber < graph.ops.numVertices) //currentSatisfiedNodesNumber < TOPK &&; find top k or whole graph iteration end    {

  {
      //println("412 starQueryGraphbfsTraverse iterationCount: ", iterationCount)
      val msgs: VertexRDD[(VD, Map[VertexId, NodeInfo])] = g.aggregateMessages[(VD, Map[VertexId, NodeInfo])](
        triplet => {
          val srcNodeMap = triplet.srcAttr._2
          //println("266 starQueryGraphbfsTraverse srcNodeMap: ", srcNodeMap)
          var dstNodeMap = triplet.dstAttr._2
          var newdstNodeMap = dstNodeMap
          //println("273 starQueryGraphbfsTraverse newdstNodeMap: "+  triplet.srcAttr._1.toString.toInt+ "   " + dstTypeId)
          specificNodeIdLst.foreach((specificNodeIdType: (VertexId, Int)) => 
            //val sourceIdType = triplet.srcAttr._1
            //consider bound with RED.id 
            if (srcNodeMap(specificNodeIdType._1).visitedColor != RED.id && srcNodeMap(specificNodeIdType._1).spDistance != Long.MaxValue && srcNodeMap(specificNodeIdType._1).spDistance + 1  < dstNodeMap(specificNodeIdType._1).spDistance)
            {
              val specificNodeId = specificNodeIdType._1
              val specNodeIdType = specificNodeIdType._2     //specific node type is vlunerablity 
              if (getHierarchicalInheritance(specNodeIdType, dstTypeId)){
                //update spDist,  parentId, and hierachical level distance
                val changedEdgeLevel: Int = triplet.attr.toString.toInt
                val tmpNodeInfo = srcNodeMap(specificNodeId).copy(spDistance = srcNodeMap(specificNodeId).spDistance+1,
                                   hierLevelDifference = srcNodeMap(specificNodeId).hierLevelDifference + changedEdgeLevel, parentId = triplet.srcId)  
                //update dstNodeMap 
                newdstNodeMap += (specificNodeId -> tmpNodeInfo)
               
                //println("282 starQueryGraphbfsTraverse newdstNodeMap: "+ triplet.srcId+ triplet.dstId+ newdstNodeMap)
              }
              else{
                  
                  //update spDist and parentId only
                  val tmpNodeInfo = srcNodeMap(specificNodeId).copy(spDistance = srcNodeMap(specificNodeId).spDistance+1, parentId = triplet.srcId)  
                  //update dstNodeMap 
                  newdstNodeMap += (specificNodeId -> tmpNodeInfo)
                  //val currentNodeTypeId = triplet.dstAttr._1 
                  //if (specificNodeIdType == 1)
                  //    println("295 starQueryGraphbfsTraverseWithBoundPruning newdstNodeMap: ", specificNodeIdType, dstTypeId)
              }
              val currentNodeTypeId = triplet.dstAttr._1 
              triplet.sendToDst((currentNodeTypeId, newdstNodeMap))
                          
            }
          )
        },
        
        (a, b) => {                  //aggregate message;  reduce function;    different src nodes
          val nodeTypeId = a._1
          val nodeMapA = a._2
          val nodeMapB = b._2
          var newMap = Map[VertexId, NodeInfo]()
          specificNodeIdLst.foreach((specificNodeIdType: (VertexId, Int)) =>

           //keep current specificNodeId's map value
          if (nodeMapA(specificNodeIdType._1).spDistance < nodeMapB(specificNodeIdType._1).spDistance){  
              //update visit color,  lowerBoundCloseness Score
              val updatedLowerBoundCloseScore = calculateLowerBound(specificNodeIdType._1, nodeMapA)
               
              val tmpNodeInfo = nodeMapA(specificNodeIdType._1).copy(visitedColor = GREY.id, lowerBoundCloseScore = updatedLowerBoundCloseScore)  //update color visited
              newMap += (specificNodeIdType._1 -> tmpNodeInfo)       //update key -> value
              
          }
          else if (nodeMapA(specificNodeIdType._1).spDistance == nodeMapB(specificNodeIdType._1).spDistance){   
              //update bound
              val updatedLowerBoundCloseScoreA = calculateLowerBound(specificNodeIdType._1, nodeMapA)
              val updatedLowerBoundCloseScoreB = calculateLowerBound(specificNodeIdType._1, nodeMapB)
              
              val updatedLowerBoundCloseScore =  updatedLowerBoundCloseScoreA + updatedLowerBoundCloseScoreB
              val tmpNodeInfo = nodeMapA(specificNodeIdType._1).copy(spNumber = nodeMapA(specificNodeIdType._1).spNumber+1, visitedColor = GREY.id, 
                                                                     lowerBoundCloseScore = updatedLowerBoundCloseScore)  //update spNumber
              newMap += (specificNodeIdType._1 -> tmpNodeInfo)
             // val newMap = {newMap}
          }
          else{
            
              //nodeMapB(specificNodeIdType._1).lowerBoundCloseScore + ....
              val updatedLowerBoundCloseScore = calculateLowerBound(specificNodeIdType._1, nodeMapB)

              val tmpNodeInfo = nodeMapB(specificNodeIdType._1).copy(visitedColor = GREY.id, lowerBoundCloseScore = updatedLowerBoundCloseScore)  //update color visited
              newMap += (specificNodeIdType._1 -> tmpNodeInfo)
              
          })
         //print ("286: starQueryGraphbfsTraverseWithBoundPruning: ", newMap)
         (nodeTypeId, newMap)
          
        }
    ).cache()
      
    //no updated messages
    if (msgs.count == 0)
      //(topKResultRdd, pathAnswerRdd)
       topKResultRdd                         //only return topKResultRdd
       
    g = g.ops.joinVertices(msgs) {
        (nodeId, oldAttr, newAttr) =>
        val nodeOldMap = oldAttr._2
        val nodeNewMap = newAttr._2
        val nodeTypeId = newAttr._1
        
        var dstNodeTypeVisitFlag = true
        var newMap = Map[VertexId, NodeInfo]()           //initialization 
        specificNodeIdLst.foreach{(specificNodeIdType: (VertexId, Int)) =>
          if (nodeNewMap(specificNodeIdType._1).spDistance <= nodeOldMap(specificNodeIdType._1).spDistance)
          {
              val spDistance = nodeNewMap(specificNodeIdType._1).spDistance
              val spNumber = nodeNewMap(specificNodeIdType._1).spNumber
              val newhierLevelDifference =  nodeNewMap(specificNodeIdType._1).hierLevelDifference*(-1)          //-1*hierLevelDifference； downward inheritance
              val newClosenessScore = calculateClosenessScore(spDistance, spNumber, newhierLevelDifference)
              val newLowerBoundCScore = math.min(N*scala.math.pow(ALPHA, (spDistance-newhierLevelDifference)), nodeNewMap(specificNodeIdType._1).lowerBoundCloseScore)
              
              val newUpperBoundCScore = calculateUpperBound(newLowerBoundCScore, spDistance, newhierLevelDifference)
              val tmpNodeInfo = nodeNewMap(specificNodeIdType._1).copy(closenessNodeScore = newClosenessScore, hierLevelDifference = newhierLevelDifference,
                                                                 lowerBoundCloseScore = newLowerBoundCScore, upperBoundCloseScore = newUpperBoundCScore)  //update closenessNodeScore 
              newMap += (specificNodeIdType._1 -> tmpNodeInfo)
          }
          else
          {
              newMap += (specificNodeIdType._1 -> nodeOldMap(specificNodeIdType._1))
          }
           
          if (newMap(specificNodeIdType._1).visitedColor != GREY.id)
          {
             dstNodeTypeVisitFlag = false
          }
           
        }
          
          //test println
         // if (nodeId == 40)
         (nodeTypeId, newMap)

      }.cache()
      
      //check all the nodes that have been updated, i.e. visited 
      /*
      val allNodesVisited =  g.vertices.filter{ case x=>
        val nodeMap = x._2._2
        
        //judge the nodes is visited from all the specific nodes
        def getAllVisiteFlag(nodeMap: Map[VertexId, NodeInfo]) ={             //define function
          var visitedFlag = true
          for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
           if (nodeInfo.spDistance == Long.MaxValue)
             visitedFlag = false
          }
          visitedFlag
        }
        getAllVisiteFlag(nodeMap)
      }
      */
     val allNodesVisited =  g.vertices.filter{ case x=>
        val nodeMap = x._2._2
        
        //judge the nodes is visited from all the specific nodes
        def getAllVisiteFlag(nodeMap: Map[VertexId, NodeInfo]) ={             //define function
          var visitedFlag = false
          for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
              if (nodeInfo.spDistance != Long.MaxValue)        //any one exist
              visitedFlag = true
          }
          visitedFlag
        }
        getAllVisiteFlag(nodeMap)
      }
      
      
       g = setnodeIdColorForBound(allNodesVisited, g)                 //update bounding nodes color
  
        
      //how many nodes have been visited from all specific nodes
      allNodesVisitedNumber =  allNodesVisited.count()
      
      //print ("562: starQueryGraphbfsTraverseWithBoundPruning currentIterateNodeResult: ", allNodesVisitedNumber, twoPreviousOldAllNodesVisitedNumber, oldAllNodesVisitedNumber)
      
      twoPreviousOldAllNodesVisitedNumber = oldAllNodesVisitedNumber
      
      val currentIterateNodeResult = allNodesVisited.filter(x => x._2._1 == dstTypeId)
      // currentIterateNodeResult.take(10).foreach(println)
      
      //how many satisfied nodes into top K nodes list
      currentSatisfiedNodesNumber = currentIterateNodeResult.count()
      //print ("649: starQueryGraphbfsTraverseWithBoundPruning currentIterateNodeResult: ", allNodesVisitedNumber, currentSatisfiedNodesNumber)
      
      //statisitics of iteration number
      iterationCount += 1

      oldAllNodesVisitedNumber = allNodesVisitedNumber
      //print ("604: starQueryGraphbfsTraverseWithBoundPruning twoPreviousOldAllNodesVisitedNumber oldAllNodesVisitedNumber: ", allNodesVisitedNumber, twoPreviousOldAllNodesVisitedNumber, oldAllNodesVisitedNumber)
      
          
      //put result into topK final list top K
      //var transferedResultRdd: RDD[(VertexId, Int)] = sc.emptyRDD()
      //whether it has topK candidates satisfied.
      if (currentSatisfiedNodesNumber <= TOPK)
      {
          topKResultRdd =  currentIterateNodeResult.map(x=>
          (x._1, (calculateNodeScoreStarquery(x._2._2), calculateMatchingScoreLowerBound(x._2._2),  calculateMatchingScoreUpperBound(x._2._2), x._2._1.toString.toInt, x._2._2)))
        
          //print ("584: starQueryGraphbfsTraverseWithBoundPruning topKResultRdd： "+ TOPK+ "--" + topKResultRdd.count)
          //topKResultRdd.collect().foreach(println)
          
         // topKResultRdd
      }
      else{              //there are k element in the list
        //get topK union by calculateNodesScoreStarquery
          val topKResultRddArray =  currentIterateNodeResult.map(x=>
          (x._1, (calculateNodeScoreStarquery(x._2._2), calculateMatchingScoreLowerBound(x._2._2),  calculateMatchingScoreUpperBound(x._2._2), x._2._1.toString.toInt, x._2._2))).takeOrdered(TOPK)(Ordering[Double].reverse.on(x=>x._2._1))    //sort by matching score calculateNodeScoreStarquery result
       
          //get the kth smallest lower bound score in the topKResultRddArray
          topKKthLowerBoundScore = topKResultRddArray.sortBy(x=>x._2._2).head._2._2       //sorted by lower bound matching score
           
          topKResultRdd = sc.parallelize(topKResultRddArray)                //transfer to RDD data structure
           //print ("598: starQueryGraphbfsTraverseWithBoundPruning topKResultRdd: "+ TOPK + "----" + topKResultRdd.count, topKKthLowerBoundScore)
         // topKResultRdd.collect().foreach(println)
        //  topKResultRdd
      } 
    
      
    }
     
    
    
    val endTime = System.currentTimeMillis()   
    //println("673 starQueryGraphbfsTraverseWithBoundPruning runtime: "+ (endTime-startTime) + " ms")
    if (runTimeoutputFilePath != null)
    {
        
      val runtTimefile = new File(runTimeoutputFilePath)
      val bw = new BufferedWriter(new FileWriter(runtTimefile))
      bw.write("TOP " + TOPK.toString + " " + "runtime: "+ (endTime-startTime).toString + " ms\n" )
      bw.close()
    }

    //get shortest path for the top K answer:
    pathAnswerRdd = getPathforAnswers(sc, topKResultRdd, g)
    
    (topKResultRdd, pathAnswerRdd)
   // topKResultRdd                         //only return topKResultRdd
  }
 
  
  
 /*
//star query traverse with pruning
// starQueryGraphbfsTraverseWithBoundPruning
def starQueryGraphbfsTraverseWithBoundPruning[VD, ED](sc: SparkContext, graph: Graph[VD, ED], specificNodeIdLst: List[(VertexId, Int)], dstTypeId: Int, runTimeoutputFilePath: String) = {
     
    val startTime = System.currentTimeMillis()              //System.nanoTime()

    //Vertex's  property is (VD, Map[VertexId, NodeInfo]) ; VD is the node property-- nodeIdType here; Map's key: specificNodeId, value is NodeInfo
    var g: Graph[(VD, Map[VertexId, NodeInfo]), ED] =
      graph.mapVertices((id, nodeIdType) => (nodeIdType, 
                                             specificNodeIdLst.map(specificNodeIdType=> specificNodeIdType._1-> NodeInfo(specificNodeIdType._1, 
                                                                                                                         specificNodeIdType._2, if (id == specificNodeIdType._1) 0 else Long.MaxValue, if (id == specificNodeIdType._1) 1 else 0, 0, 0.0, 0, if (id == specificNodeIdType._1) GREY.id else WHITE.id, if (id == specificNodeIdType._1) 1.0 else 0.0,  if (id == specificNodeIdType._1) 1 else {N*scala.math.pow(ALPHA, 1)})).toMap
      )).cache()
                                 
    // g.vertices.take(5).foreach(println)
    //println("486 starQueryGraphbfsTraverseWithBoundPruning :  \n")
    //iterations for bfs begin
    var iterationCount = 0
    var currentSatisfiedNodesNumber: Long = 0L                   //number of dest type nodes that visited
    var allNodesVisitedNumber: Long = 0L                         // all nodes visited
    var oldAllNodesVisitedNumber: Long = -1L                     //previous iteration nodes visited
    var twoPreviousOldAllNodesVisitedNumber: Long = -2L          //previous and previous iteration nodes visited
    
    var topKResultRdd: RDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))] = sc.emptyRDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))]           //Return Result RDD, (nodeId, matchingScore, lowerBound, upperBound, nodeMap)

    
    var pathAnswerRdd: RDD[(VertexId, Map[VertexId, ListBuffer[VertexId]])]  = sc.emptyRDD[(VertexId, Map[VertexId, ListBuffer[VertexId]])]           //Return Result RDD, (nodeId, matchingScore, lowerBound, upperBound, nodeMap)

    val dstNodesNumberGraph = graph.vertices.filter(x=>x._2 == dstTypeId)
    //println("504 starQueryGraphbfsTraverseWithBoundPruning :  ", dstNodesNumberGraph.count)
   
  //no value change or all the destination node has been visited
  while (twoPreviousOldAllNodesVisitedNumber != oldAllNodesVisitedNumber && currentSatisfiedNodesNumber < dstNodesNumberGraph.count && allNodesVisitedNumber < graph.ops.numVertices) //currentSatisfiedNodesNumber < TOPK &&; find top k or whole graph iteration end    {
  //while (currentSatisfiedNodesNumber < dstNodesNumberGraph.count && allNodesVisitedNumber < graph.ops.numVertices) //currentSatisfiedNodesNumber < TOPK &&; find top k or whole graph iteration end    {

  {
      //println("412 starQueryGraphbfsTraverse iterationCount: ", iterationCount)
      val msgs: VertexRDD[(VD, Map[VertexId, NodeInfo])] = g.aggregateMessages[(VD, Map[VertexId, NodeInfo])](
        triplet => {
          val srcNodeMap = triplet.srcAttr._2
          //println("266 starQueryGraphbfsTraverse srcNodeMap: ", srcNodeMap)
          var dstNodeMap = triplet.dstAttr._2
          var newdstNodeMap = dstNodeMap
          //println("273 starQueryGraphbfsTraverse newdstNodeMap: "+  triplet.srcAttr._1.toString.toInt+ "   " + dstTypeId)
          specificNodeIdLst.foreach((specificNodeIdType: (VertexId, Int)) => 
            //val sourceIdType = triplet.srcAttr._1
            //consider bound with RED.id 
            if (srcNodeMap(specificNodeIdType._1).visitedColor != RED.id && srcNodeMap(specificNodeIdType._1).spDistance != Long.MaxValue && srcNodeMap(specificNodeIdType._1).spDistance + 1  < dstNodeMap(specificNodeIdType._1).spDistance)
            {
              val specificNodeId = specificNodeIdType._1
              val specNodeIdType = specificNodeIdType._2     //specific node type is vlunerablity 
              if (getHierarchicalInheritance(specNodeIdType, dstTypeId)){
                //update spDist,  parentId, and hierachical level distance
                val changedEdgeLevel: Int = triplet.attr.toString.toInt
                val tmpNodeInfo = srcNodeMap(specificNodeId).copy(spDistance = srcNodeMap(specificNodeId).spDistance+1,
                                   hierLevelDifference = srcNodeMap(specificNodeId).hierLevelDifference + changedEdgeLevel, parentId = triplet.srcId)  
                //update dstNodeMap 
                newdstNodeMap += (specificNodeId -> tmpNodeInfo)
               
                //println("282 starQueryGraphbfsTraverse newdstNodeMap: "+ triplet.srcId+ triplet.dstId+ newdstNodeMap)
              }
              else{
                  
                  //update spDist and parentId only
                  val tmpNodeInfo = srcNodeMap(specificNodeId).copy(spDistance = srcNodeMap(specificNodeId).spDistance+1, parentId = triplet.srcId)  
                  //update dstNodeMap 
                  newdstNodeMap += (specificNodeId -> tmpNodeInfo)
                  //val currentNodeTypeId = triplet.dstAttr._1 
                  //if (specificNodeIdType == 1)
                  //    println("295 starQueryGraphbfsTraverseWithBoundPruning newdstNodeMap: ", specificNodeIdType, dstTypeId)
              }
              val currentNodeTypeId = triplet.dstAttr._1 
              triplet.sendToDst((currentNodeTypeId, newdstNodeMap))
                          
            }
          )
        },
        
        (a, b) => {                  //aggregate message;  reduce function;    different src nodes
          val nodeTypeId = a._1
          val nodeMapA = a._2
          val nodeMapB = b._2
          var newMap = Map[VertexId, NodeInfo]()
          specificNodeIdLst.foreach((specificNodeIdType: (VertexId, Int)) =>

           //keep current specificNodeId's map value
          if (nodeMapA(specificNodeIdType._1).spDistance < nodeMapB(specificNodeIdType._1).spDistance){  
              //update visit color,  lowerBoundCloseness Score
              val updatedLowerBoundCloseScore = calculateLowerBound(specificNodeIdType._1, nodeMapA)
               
              val tmpNodeInfo = nodeMapA(specificNodeIdType._1).copy(visitedColor = GREY.id, lowerBoundCloseScore = updatedLowerBoundCloseScore)  //update color visited
              newMap += (specificNodeIdType._1 -> tmpNodeInfo)       //update key -> value
              
          }
          else if (nodeMapA(specificNodeIdType._1).spDistance == nodeMapB(specificNodeIdType._1).spDistance){   
              //update bound
              val updatedLowerBoundCloseScoreA = calculateLowerBound(specificNodeIdType._1, nodeMapA)
              val updatedLowerBoundCloseScoreB = calculateLowerBound(specificNodeIdType._1, nodeMapB)
              
              val updatedLowerBoundCloseScore =  updatedLowerBoundCloseScoreA + updatedLowerBoundCloseScoreB
              val tmpNodeInfo = nodeMapA(specificNodeIdType._1).copy(spNumber = nodeMapA(specificNodeIdType._1).spNumber+1, visitedColor = GREY.id, 
                                                                     lowerBoundCloseScore = updatedLowerBoundCloseScore)  //update spNumber
              newMap += (specificNodeIdType._1 -> tmpNodeInfo)
             // val newMap = {newMap}
          }
          else{
            
              //nodeMapB(specificNodeIdType._1).lowerBoundCloseScore + ....
              val updatedLowerBoundCloseScore = calculateLowerBound(specificNodeIdType._1, nodeMapB)

              val tmpNodeInfo = nodeMapB(specificNodeIdType._1).copy(visitedColor = GREY.id, lowerBoundCloseScore = updatedLowerBoundCloseScore)  //update color visited
              newMap += (specificNodeIdType._1 -> tmpNodeInfo)
              
          })
         //print ("286: starQueryGraphbfsTraverseWithBoundPruning: ", newMap)
         (nodeTypeId, newMap)
          
        }
    ).cache()
      
    //no updated messages
    if (msgs.count == 0)
      //(topKResultRdd, pathAnswerRdd)
       topKResultRdd                         //only return topKResultRdd
       
    g = g.ops.joinVertices(msgs) {
        (nodeId, oldAttr, newAttr) =>
        val nodeOldMap = oldAttr._2
        val nodeNewMap = newAttr._2
        val nodeTypeId = newAttr._1
        
        var dstNodeTypeVisitFlag = true
        var newMap = Map[VertexId, NodeInfo]()           //initialization 
        specificNodeIdLst.foreach{(specificNodeIdType: (VertexId, Int)) =>
          if (nodeNewMap(specificNodeIdType._1).spDistance <= nodeOldMap(specificNodeIdType._1).spDistance)
          {
              val spDistance = nodeNewMap(specificNodeIdType._1).spDistance
              val spNumber = nodeNewMap(specificNodeIdType._1).spNumber
              val newhierLevelDifference =  nodeNewMap(specificNodeIdType._1).hierLevelDifference*(-1)          //-1*hierLevelDifference； downward inheritance
              val newClosenessScore = calculateClosenessScore(spDistance, spNumber, newhierLevelDifference)
              val newLowerBoundCScore = math.min(N*scala.math.pow(ALPHA, (spDistance-newhierLevelDifference)), nodeNewMap(specificNodeIdType._1).lowerBoundCloseScore)
              
              val newUpperBoundCScore = calculateUpperBound(newLowerBoundCScore, spDistance, newhierLevelDifference)
              val tmpNodeInfo = nodeNewMap(specificNodeIdType._1).copy(closenessNodeScore = newClosenessScore, hierLevelDifference = newhierLevelDifference,
                                                                 lowerBoundCloseScore = newLowerBoundCScore, upperBoundCloseScore = newUpperBoundCScore)  //update closenessNodeScore 
              newMap += (specificNodeIdType._1 -> tmpNodeInfo)
          }
          else
          {
              newMap += (specificNodeIdType._1 -> nodeOldMap(specificNodeIdType._1))
          }
           
          if (newMap(specificNodeIdType._1).visitedColor != GREY.id)
          {
             dstNodeTypeVisitFlag = false
          }
           
        }
          
          //test println
         // if (nodeId == 40)
         (nodeTypeId, newMap)

      }.cache()
      
      //check all the nodes that have been updated, i.e. visited 
      /*
      val allNodesVisited =  g.vertices.filter{ case x=>
        val nodeMap = x._2._2
        
        //judge the nodes is visited from all the specific nodes
        def getAllVisiteFlag(nodeMap: Map[VertexId, NodeInfo]) ={             //define function
          var visitedFlag = true
          for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
           if (nodeInfo.spDistance == Long.MaxValue)
             visitedFlag = false
          }
          visitedFlag
        }
        getAllVisiteFlag(nodeMap)
      }
      */
     val allNodesVisited =  g.vertices.filter{ case x=>
        val nodeMap = x._2._2
        
        //judge the nodes is visited from all the specific nodes
        def getAllVisiteFlag(nodeMap: Map[VertexId, NodeInfo]) ={             //define function
          var visitedFlag = false
          for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
              if (nodeInfo.spDistance != Long.MaxValue)        //any one exist
              visitedFlag = true
          }
          visitedFlag
        }
        getAllVisiteFlag(nodeMap)
      }
      
      
       g = setnodeIdColorForBound(allNodesVisited, g)                 //update bounding nodes color
  
        
      //how many nodes have been visited from all specific nodes
      allNodesVisitedNumber =  allNodesVisited.count()
      
      //print ("562: starQueryGraphbfsTraverseWithBoundPruning currentIterateNodeResult: ", allNodesVisitedNumber, twoPreviousOldAllNodesVisitedNumber, oldAllNodesVisitedNumber)
      
      twoPreviousOldAllNodesVisitedNumber = oldAllNodesVisitedNumber
      
      val currentIterateNodeResult = allNodesVisited.filter(x => x._2._1 == dstTypeId)
      // currentIterateNodeResult.take(10).foreach(println)
      
      //how many satisfied nodes into top K nodes list
      currentSatisfiedNodesNumber = currentIterateNodeResult.count()
      //print ("649: starQueryGraphbfsTraverseWithBoundPruning currentIterateNodeResult: ", allNodesVisitedNumber, currentSatisfiedNodesNumber)
      
      //statisitics of iteration number
      iterationCount += 1

      oldAllNodesVisitedNumber = allNodesVisitedNumber
      //print ("604: starQueryGraphbfsTraverseWithBoundPruning twoPreviousOldAllNodesVisitedNumber oldAllNodesVisitedNumber: ", allNodesVisitedNumber, twoPreviousOldAllNodesVisitedNumber, oldAllNodesVisitedNumber)
      
    }
     
     
     val currentIterateNodeResultEndRdd = g.vertices.filter{ case x=>
        val nodeMap = x._2._2
        
        //judge the nodes is visited from all the specific nodes
        def getAllVisiteFlag(nodeMap: Map[VertexId, NodeInfo]) ={             //define function
          var visitedFlag = false
          for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
              if (nodeInfo.spDistance != Long.MaxValue)        //any one exist
              visitedFlag = true
          }
          visitedFlag
        }
        getAllVisiteFlag(nodeMap) && x._2._1 == dstTypeId
      }
    
    //put result into topK final list top K
      //var transferedResultRdd: RDD[(VertexId, Int)] = sc.emptyRDD()
      //whether it has topK candidates satisfied.
      if (currentSatisfiedNodesNumber <= TOPK)
      {
          topKResultRdd =  currentIterateNodeResultEndRdd.map(x=>
          (x._1, (calculateNodeScoreStarquery(x._2._2), calculateMatchingScoreLowerBound(x._2._2),  calculateMatchingScoreUpperBound(x._2._2), x._2._1.toString.toInt, x._2._2)))
        
          //print ("584: starQueryGraphbfsTraverseWithBoundPruning topKResultRdd： "+ TOPK+ "--" + topKResultRdd.count)
          //topKResultRdd.collect().foreach(println)
          
         // topKResultRdd
      }
      else{              //there are k element in the list
        //get topK union by calculateNodesScoreStarquery
          val topKResultRddArray =  currentIterateNodeResultEndRdd.map(x=>
          (x._1, (calculateNodeScoreStarquery(x._2._2), calculateMatchingScoreLowerBound(x._2._2),  calculateMatchingScoreUpperBound(x._2._2), x._2._1.toString.toInt, x._2._2))).takeOrdered(TOPK)(Ordering[Double].reverse.on(x=>x._2._1))    //sort by matching score calculateNodeScoreStarquery result
       
          //get the kth smallest lower bound score in the topKResultRddArray
          topKKthLowerBoundScore = topKResultRddArray.sortBy(x=>x._2._2).head._2._2       //sorted by lower bound matching score
           
          topKResultRdd = sc.parallelize(topKResultRddArray)                //transfer to RDD data structure
           //print ("598: starQueryGraphbfsTraverseWithBoundPruning topKResultRdd: "+ TOPK + "----" + topKResultRdd.count, topKKthLowerBoundScore)
         // topKResultRdd.collect().foreach(println)
        //  topKResultRdd
      } 
      
    val endTime = System.currentTimeMillis()   
    println("673 starQueryGraphbfsTraverseWithBoundPruning runtime: "+ (endTime-startTime) + " ms")
    if (runTimeoutputFilePath != null)
    {
        
      val runtTimefile = new File(runTimeoutputFilePath)
      val bw = new BufferedWriter(new FileWriter(runtTimefile))
      bw.write("TOP " + TOPK.toString + " " + "runtime: "+ (endTime-startTime).toString + " ms\n" )
      bw.close()
    }

    //get shortest path for the top K answer:
    pathAnswerRdd = getPathforAnswers(sc, topKResultRdd, g)
    
    //(topKResultRdd, pathAnswerRdd)
    topKResultRdd                         //only return topKResultRdd
  }
  
  */
  
  //star query backup
  /*
  def starQueryExeute01[VD, ED](sc: SparkContext, graph: Graph[VD, ED], specificNodeIdLst: List[(VertexId,Int)], dstTypeId: Int, inputFileNodeInfoPath: String, outputFile: String) = {
    
    val topKResultRdd = starQueryGraphbfsTraverse(sc, graph, specificNodeIdLst, dstTypeId)

    //topKResultRdd.count()
    val nodeInfoRdd =  graphInputCommon.readNodeInfoName(sc, inputFileNodeInfoPath)
    //join (k,v), (k, w) => k, (v, w)
    val resultStarQueryRdd = nodeInfoRdd.join(topKResultRdd)
    print ("385: starQueryExeute resultStarQueryRdd: \n")
    
    resultStarQueryRdd.collect().foreach(println)
    resultStarQueryRdd.coalesce(1).saveAsTextFile(outputFile)        //coalesce into 1 file, it is small data output
  }
  */
 
  def judgeEdgeInPairList(newNodePair: List[String],  srcId: VertexId, dstId: VertexId) = {
    val inStr = srcId.toString + "-" + dstId.toString
    if (newNodePair.contains(inStr))
      true
    else
      false
  }
        
  //delet node edge for experimentation verification
  def preProcessGraphDeleteEdge[VD, ED](graph: Graph[VD, ED],  nodePairs: List[(VertexId, VertexId)]) = {
    val newNodePair = nodePairs.map(x => x._1.toString + "-" + x._2.toString)         //list map
    
    val newGraph =  graph.subgraph(epred = e => !judgeEdgeInPairList(newNodePair, e.srcId, e.dstId))
    
    // val newGraph = Graph.apply(graph.vertices, graph.edges)
   // print ("493: judgeEdgeInPairList topKResultRdd \n")
   // newGraph.edges.take(10).foreach(println)
    
    newGraph
  
  }
  
  
  
  //execute main star query
  def starQueryExeute[VD, ED](sc: SparkContext, graph: Graph[VD, ED], specificNodeIdLst: List[(VertexId,Int)], dstTypeId: Int, inputFileNodeInfoPath: String, outputFilePath: String, runTimeoutputFilePath: String) = {
    
   // val newGraph = preProcessGraphDeleteEdge(graph, List((40, 58)))      //preprocess for different query
    
    //val topKResultRdd = starQueryGraphbfsTraverseWithBoundPruning(sc, graph, specificNodeIdLst, dstTypeId, runTimeoutputFilePath)
    
    val answers = starQueryGraphbfsTraverseWithBoundPruning(sc, graph, specificNodeIdLst, dstTypeId, runTimeoutputFilePath)
    val topKResultRdd = answers._1
    val pathAnswerRdd = answers._2

    //topKResultRdd.count()
    val nodeInfoRdd =  graphInputCommon.readNodeInfoName(sc, inputFileNodeInfoPath)
    
    val resultStarQueryRdd = nodeInfoRdd.join(topKResultRdd)    //join (k,v), (k, w) => k, (v, w)
    //print ("385: starQueryExeute resultStarQueryRdd: \n")
    
    //resultStarQueryRdd.collect().foreach(println)
    //resultStarQueryRdd.coalesce(1).saveAsTextFile(outputFileNode)        //coalesce into 1 file, it is small data output
    print ("422: starQueryExeute pathAnswerRdd: \n")
    pathAnswerRdd.collect().foreach(println)
    // outputFilePath
    resultStarQueryRdd.coalesce(1).saveAsTextFile(outputFilePath)        //coalesce into 1 file, it is small data output

   // topKResultRdd.coalesce(1).saveAsTextFile(outputFilePath)        //coalesce into 1 file, it is small data output

  }
  
   
}


