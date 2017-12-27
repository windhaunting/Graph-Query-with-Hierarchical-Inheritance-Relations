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

import main.scala.hierarchialQuery.nodeTypeProductEnum._
import main.scala.hierarchialQuery.nodeTypeDBLPEnum._
import main.scala.hierarchialQuery.visitedColorEnum._
import main.scala.hierarchialQuery.nodeTypeSyntheticGraphEnum._


import scala.reflect.ClassTag
import scala.util.control.Breaks._
import java.io._

object starQuery {
 
  var TOPK = 0                 //top K candidate answer, set by main function parameter
  val ALPHA = 0.9               //propagation factor
  val N = math.pow(ALPHA, (-1)*ALPHA)   //1/ALPHA1.05
  val numTasks = 8                   //how many task for one core can execute in parallell
  val BETA = 0.8                    //attenutation for hierarchical level difference causing the score reduction.
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

  //given the node types, hierarchical inheritance or not (hierarchical or generic relations); databaseType: ciso product : 0, dblp : 1
  def getHierarchicalInheritance(nodeIdType1: Int, nodeIdType2: Int, databaseType: Int, hierarchialRelation: Boolean) = {
    //print ("189 getHierarchicalInheritance PRODUCT.id: ", PRODUCT.id +" " + VULNERABILITY.id)

    //test no hierarchical relations, return false always
    //false 
    if (!hierarchialRelation){
        false
    }
    else{
        //cisco product dataset
        if (databaseType == 0){
          if ((nodeIdType1 == PRODUCT.id && nodeIdType2 == VULNERABILITY.id) || (nodeIdType1 == VULNERABILITY.id && nodeIdType2 == PRODUCT.id)){
            true
          }
          else{
            false
          } 
        }
        else if (databaseType == 1){      //extended dblp dataset
          if (nodeIdType1 == TOPIC.id || nodeIdType2 == TOPIC.id){
            true
          }
          else{
            false
          } 
        }
        else if (databaseType == 2){      //synthetic dataset
          if (nodeIdType1 == TYPE0INHERIT.id || nodeIdType2 == TYPE1INHERIT.id || nodeIdType1 == TYPE1INHERIT.id || nodeIdType2 == TYPE0INHERIT.id){
            true
          }
          else{
            false
          } 
       }
       else{
          false
       }
    }
    
}
  
  
//get the closeness score from the parameters
  def calculateClosenessScore(spDistance: Long, spNumber: Long, parentNodeHierarchicalLeve: Double, currntHierLevelDifference: Double) = {
    
    val closeScore: Double = math.min(N*scala.math.pow(ALPHA, (spDistance-parentNodeHierarchicalLeve)), spNumber*scala.math.pow(ALPHA, (spDistance - currntHierLevelDifference).toDouble))                         //just use math.pow( ) deprecated? http://alvinalexander.com/scala/scala-math-power-exponent-exponentiation-function
    
   // val closeScore: Double = math.min(scala.math.pow(ALPHA, (spDistance-hierLevelDifference-1)), spNumber*scala.math.pow(ALPHA, (spDistance -hierLevelDifference).toDouble))                         //just use math.pow( ) deprecated? http://alvinalexander.com/scala/scala-math-power-exponent-exponentiation-function

    closeScore
    
  }

  //calculate node score given all specific nodes in nodeMap
  def calculateNodeScoreStarquery(nodeMap: Map[VertexId, NodeInfo]) = {
    val nodeNum: Double = nodeMap.size
    var sumClosenessScore: Double = 0.0
    for ((specNodeId, nodeInfo) <- nodeMap){
      sumClosenessScore += nodeInfo.closenessNodeScore
    }
    //sumClosenessScore/nodeNum
    sumClosenessScore
  }
  
  
    
         
  //get the shortest path of visited nodes from specic node
  def getPathforAnswers[VD, ED](sc: SparkContext, topkResultRdd: RDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))], graphExtended: Graph[(VD, Map[VertexId, NodeInfo]), ED] ) = {
    var pathAnswersLst = new ListBuffer[(VertexId, Map[VertexId, ListBuffer[VertexId]])]()
    val topkResultList = topkResultRdd.collect().toList
    //print ("227 getPathforAnswers ttttttttt: " + topkResultList)
    
    for (lstTuple <- topkResultList){
      val nodeId = lstTuple._1
      val nodeMap = lstTuple._2._5           //
      
      //define getVisitedNodesPath function
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
                  
                   tmpPathLstBuffer.insert(0, prevNodeId)          //  iterate to insert prarent node into head

                 }
                 else{
                   
                    tmpPathLstBuffer.insert(0, specific)
                 }
                 //print ("262 getPathforAnswers pathLstMap: " +  prevNodeId + " " + tmpPathLstBuffer.head)

                 
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
 
  

  /*
//inner local update lower bound closeness score
def calculateLowerBound[VD, ED](specificNodeId: VertexId, nodeMap: Map[VertexId, NodeInfo], prevIterCurrentNodeLowerScore: Double) = {
  
    val prevParentNodeLowerScore = nodeMap(specificNodeId).lowerBoundCloseScore
    val prevIterNeighborHierLevelDistance = nodeMap(specificNodeId).hierLevelDifference
    
    var updatedLowerBoundCloseScore = 0.0
    if (prevIterCurrentNodeLowerScore > 0)
    {
        updatedLowerBoundCloseScore = prevIterCurrentNodeLowerScore
    }
    else
    {
        //get previous visited neighbor lower score; aggregate is done in the graphX aggregateMessage
        updatedLowerBoundCloseScore = scala.math.pow(ALPHA, 1-prevIterNeighborHierLevelDistance) * prevParentNodeLowerScore
    }
    updatedLowerBoundCloseScore
    
}


    
//global update lower bound closeness score
def calculateUpperBound(currentLowerBound: Double, iterationNumber: Long, hierLevelDifference: Double) = {
   
    var updatedUpperBoundCloseScore = 0.0
    if (currentLowerBound > 0)
      {
        updatedUpperBoundCloseScore = currentLowerBound
      }
    else
      {
        updatedUpperBoundCloseScore = scala.math.pow(ALPHA, (iterationNumber-hierLevelDifference))       //hierLevelDifference includes BETA already
      }
    updatedUpperBoundCloseScore    
    
}
  */

  
 //calculate node similarity lower bound when the node visited from one specificNodeId
 def calculateNodeSimilarityLowerBound[VD, ED](currentSpDistance: Long, parentNodeHierarchicalLevel: Double, prevIterCurrentNodeLowerScore: Double,  parentNodePrevIterLowerScore: Double, neighborNodehierLevelDifference: Double) = {
    var updatedLowerBoundCloseScore = 0.0
    if (prevIterCurrentNodeLowerScore > 0)
    {
      updatedLowerBoundCloseScore = prevIterCurrentNodeLowerScore
    }
    else{
        //get previous visited neighbor lower score; aggregate is done in the graphX aggregateMessage
        //  math.min(N*scala.math.pow(ALPHA, (currentSpDistance-currentHierLevelDifference)),
      updatedLowerBoundCloseScore = math.min(N*scala.math.pow(ALPHA, (currentSpDistance-parentNodeHierarchicalLevel)), scala.math.pow(ALPHA, 1-neighborNodehierLevelDifference) * parentNodePrevIterLowerScore)
    }
    updatedLowerBoundCloseScore
    
  }

 //calculate node similarity upper bound when the node visited from one specificNodeId
def calculateNodeSimilarityUpperBound[VD, ED](currentLowerBoundScore: Double, iterationNumber: Long, hierLevelDifference: Double) = {

    var updatedUpperBoundCloseScore = 0.0
    if (currentLowerBoundScore > 0)
    {
        updatedUpperBoundCloseScore = currentLowerBoundScore
    }
    else
    {
        updatedUpperBoundCloseScore = scala.math.pow(ALPHA, (iterationNumber-hierLevelDifference))       //hierLevelDifference includes BETA already
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
    //matchingScoreLowerBound/nodeNum
    matchingScoreLowerBound
  }
  
//get matching Score upper Bound from nodeMap
def calculateMatchingScoreUpperBound(nodeMap: Map[VertexId, NodeInfo]) = {
    val nodeNum: Double = nodeMap.size
    var matchingScoreUpperBound: Double = 0.0

    for ((specNodeId, nodeInfo) <- nodeMap){
      if (nodeInfo.spDistance != Long.MaxValue){                //only visited nodes need to compute score
        
         matchingScoreUpperBound += nodeInfo.upperBoundCloseScore
      }
    }
    //matchingScoreUpperBound/nodeNum
    matchingScoreUpperBound

  }
  
  
 //get all visited nodes' matching lower and upper bound score
def setnodeIdColorForBound[VD, ED](allNodesVisited: VertexRDD[(VD, Map[VertexId, NodeInfo])], g: Graph[(VD, Map[VertexId, NodeInfo]), ED]) = {
  
    val updatedNodeRdd = allNodesVisited.map{
      case (nodeId, (nodeIdType, nodeMap))=>
        val matchingScoreUpperBound = calculateMatchingScoreUpperBound(nodeMap)
        
        if (matchingScoreUpperBound != 0 && matchingScoreUpperBound <= topKKthLowerBoundScore)          //upperbound less than the kth lowerbound score
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
def starQueryGraphbfsTraverseWithBoundPruning[VD, ED](sc: SparkContext, graph: Graph[VD, ED], specificNodeIdLst: List[(VertexId, Int)], dstTypeId: Int, databaseType: Int, runTimeoutputFilePath: String, hierarchialRelation: Boolean) = {
     
    //Vertex's  property is (VD, Map[VertexId, NodeInfo]) ; VD is the node property-- nodeIdType here; Map's key: specificNodeId, value is NodeInfo; ED is edge property
    var g: Graph[(VD, Map[VertexId, NodeInfo]), ED] =
      graph.mapVertices((id, nodeIdType) => (nodeIdType, 
                                             specificNodeIdLst.map(specificNodeIdType=> specificNodeIdType._1-> NodeInfo(specificNodeIdType._1, 
                                                                                                                         specificNodeIdType._2, if (id == specificNodeIdType._1) 0 else Long.MaxValue, if (id == specificNodeIdType._1) 1 else 0, 0.0, 0.0, 0, if (id == specificNodeIdType._1) GREY.id else WHITE.id, if (id == specificNodeIdType._1) 1.0 else 0.0,  if (id == specificNodeIdType._1) 1.0 else 1.0)).toMap
      )).cache()
                                 
    // g.vertices.take(5).foreach(println)
    //println("486 starQueryGraphbfsTraverseWithBoundPruning :  \n")
    //iterations for bfs begin
    var iterationCount = 0
                   //previous iteration nodes visited
   // var twoPreviousOldAllNodesVisitedNumber: Long = -2L          //previous and previous iteration nodes visited
    
    var topKResultRdd: RDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))] = sc.emptyRDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))]           //Return Result RDD, (nodeId, matchingScore, lowerBound, upperBound, nodeMap)

    var pathAnswerRdd: RDD[(VertexId, Map[VertexId, ListBuffer[VertexId]])]  = sc.emptyRDD[(VertexId, Map[VertexId, ListBuffer[VertexId]])]           //Return Result RDD, (nodeId, matchingScore, lowerBound, upperBound, nodeMap)

    val dstNodesNumberGraph = graph.vertices.filter(x=>x._2 == dstTypeId)            
    //println("504 starQueryGraphbfsTraverseWithBoundPruning :  ", dstNodesNumberGraph.count)
   
     var currentSatisfiedNodesNumber: Long = 0L                   //number of dest type nodes that visited
     var allNodesVisitedNumber: Long = 0L                         // all nodes visited at current iteration
    //var allNodesVisitedNumberNewTemp: Long = 0L                 // specificNodeIdLst.length // 0L                     //temp allNodesVisitedNumber
    var oldAllNodesVisitedNumber: Long = -1L      
    
    
    val startTime = System.currentTimeMillis()              //System.nanoTime()
  
  //no value change or all the destination node has been visited currentSatisfiedNodesNumber <=TOPK 
  //while (twoPreviousOldAllNodesVisitedNumber != oldAllNodesVisitedNumber && currentSatisfiedNodesNumber < dstNodesNumberGraph.count && allNodesVisitedNumber < graph.ops.numVertices) //currentSatisfiedNodesNumber < TOPK &&; find top k or whole graph iteration end    {
  //while (currentSatisfiedNodesNumber < dstNodesNumberGraph.count && allNodesVisitedNumber < graph.ops.numVertices) //currentSatisfiedNodesNumber < TOPK &&; find top k or whole graph iteration end    {
  while (oldAllNodesVisitedNumber != allNodesVisitedNumber && currentSatisfiedNodesNumber < dstNodesNumberGraph.count && allNodesVisitedNumber < graph.ops.numVertices) //currentSatisfiedNodesNumber < TOPK &&; find top k or whole graph iteration end    {

  {
      //println("412 starQueryGraphbfsTraverse iterationCount: ", iterationCount)
      val msgs = g.aggregateMessages[(VD, Map[VertexId, NodeInfo],  Map[VertexId, (Double, Double)], Map[VertexId, Double], VertexId, VertexId)](
        triplet => {
          val srcNodeMap = triplet.srcAttr._2
          //println("266 starQueryGraphbfsTraverse srcNodeMap: ", srcNodeMap)
          var dstNodeMap = triplet.dstAttr._2          //dstNodeMap
          var sendMsgFlag = false          //sendMsgFlag
          val currentNodeType = triplet.dstAttr._1  
          //println("273 starQueryGraphbfsTraverse newdstNodeMap: "+  triplet.srcAttr._1.toString.toInt+ "   " + dstTypeId)
          var newdstNodeMap = Map[VertexId, NodeInfo]()            // not dstNodeMap any more, empty first, only updated vertexId part need to be sent (message)
          var prevIterParentNodeLowerBoundsMap = Map[VertexId, (Double, Double)]()  
          var  prevIterCurrentNodeLowerBoundsMap = Map[VertexId, Double]()            //lower bound similarity score at previous iteration t-1
       
          specificNodeIdLst.foreach((specificNodeIdType: (VertexId, Int)) => 
            //val sourceIdType = triplet.srcAttr._1
            //consider bound with RED.id 
            if (dstNodeMap(specificNodeIdType._1).visitedColor != GREY.id && srcNodeMap(specificNodeIdType._1).visitedColor != RED.id && srcNodeMap(specificNodeIdType._1).spDistance != Long.MaxValue && srcNodeMap(specificNodeIdType._1).spDistance + 1  < dstNodeMap(specificNodeIdType._1).spDistance)
            {
              
               val specificNodeId = specificNodeIdType._1
               val specNodeIdType = specificNodeIdType._2     //specific node type is vlunerablity 
               if (getHierarchicalInheritance(specNodeIdType, dstTypeId, databaseType, hierarchialRelation)){
                 //update spDist,  parentId, and hierachical level distance
                 val changedEdgeLevel: Double = BETA*math.abs(triplet.attr.toString.toInt)
                 //val tmpNodeInfo = srcNodeMap(specificNodeId).copy(spDistance = srcNodeMap(specificNodeId).spDistance+1,
                 //                   hierLevelDifference = srcNodeMap(specificNodeId).hierLevelDifference + changedEdgeLevel, parentId = triplet.srcId)  

                 val tmpNodeInfo = dstNodeMap(specificNodeId).copy(spDistance = srcNodeMap(specificNodeId).spDistance+1,
                                   hierLevelDifference = srcNodeMap(specificNodeId).hierLevelDifference + changedEdgeLevel, spNumber= srcNodeMap(specificNodeId).spNumber,  parentId = triplet.srcId)  
               
                 //update dstNodeMap 
                 newdstNodeMap += (specificNodeId -> tmpNodeInfo)
                //  if (changedEdgeLevel !=0 ){
                //      println("282 starQueryGraphbfsTraverse newdstNodeMap: "+ triplet.srcId+ " " + triplet.dstId + " " + changedEdgeLevel)
                //  }
               }
               else{
                  
                  //update spDist and parentId only
                  val tmpNodeInfo = dstNodeMap(specificNodeId).copy(spDistance = srcNodeMap(specificNodeId).spDistance+1, spNumber= srcNodeMap(specificNodeId).spNumber,
                                                                    hierLevelDifference = srcNodeMap(specificNodeId).hierLevelDifference, parentId = triplet.srcId)  
                  //update dstNodeMap 
                  newdstNodeMap += (specificNodeId -> tmpNodeInfo)
                  //val currentNodeType = triplet.dstAttr._1 
                  //if (specificNodeIdType == 1)
                  //    println("295 starQueryGraphbfsTraverseWithBoundPruning newdstNodeMap: ", specificNodeIdType, dstTypeId)
               }
               
             // if ((triplet.dstId == 162456) || (triplet.dstId == 120096) || (triplet.dstId == 51308))
             // {
              //   println("401 starQueryGraphbfsTraverseWithBoundPruning newdstNodeMap: "+ specificNodeId+" srcId: "+triplet.srcId+" dstId: "+  triplet.dstId+ " srcMap: "+ triplet.srcAttr._2 + " newdstMap:" + newdstNodeMap)
              //}
               
              //get current lowerbounds that is actually the previous iteration's lower bound  t-1
              val neighborNodehierLevelDifference = BETA*math.abs(triplet.attr.toString.toInt)              //edge hierarchical level difference between srcId and dstId

              prevIterParentNodeLowerBoundsMap += (specificNodeIdType._1-> (srcNodeMap(specificNodeIdType._1).lowerBoundCloseScore, neighborNodehierLevelDifference))    // check
              prevIterCurrentNodeLowerBoundsMap += (specificNodeIdType._1-> dstNodeMap(specificNodeIdType._1).lowerBoundCloseScore)  //check
              //println("403 starQueryGraphbfsTraverseWithBoundPruning newdstNodeMap: "+ specificNodeId+" srcId: "+triplet.srcId+" dstId: "+  triplet.dstId+ " srcMap: "+ triplet.srcAttr._2 + " newdstMap:" + newdstNodeMap)
              
              sendMsgFlag = true
     
            }
          )
          
          if (sendMsgFlag)
          {   
             //  if ((triplet.dstId == 162456) || (triplet.dstId == 120096) || (triplet.dstId == 51308))
             // {
             //     println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx403 starQueryGraphbfsTraverseWithBoundPruning newdstNodeMap: " + " dstId: " +triplet.dstId)
             // }
              triplet.sendToDst((currentNodeType, newdstNodeMap, prevIterParentNodeLowerBoundsMap, prevIterCurrentNodeLowerBoundsMap, triplet.srcId, triplet.dstId))
              sendMsgFlag = false
          }
        },
        
        (a, b) => {                  //aggregate message;  reduce function;    different src nodes; if only one src, no reduction code here
          val nodeTypeId = a._1
          var nodeMapA = a._2
          var nodeMapB = b._2
          var prevIterParentNodeLowerBoundsMapA = a._3
          val prevIterParentNodeLowerBoundsMapB = b._3
          
          var prevIterCurrentLowerBoundsMapA = a._4              //Map[VertexId, Double]()
          val prevIterCurrentLowerBoundsMapB = b._4
          val srcId1 = a._5
          val srcId2 = b._5
          val dstId1 = a._6
          val dstId2 = b._6

          // val updatedSpecificIdA = a._5
         // val updatedSpecificIdB = b._5
          
          //var prevIterParentNodeLowerBoundsMap = Map[VertexId, (Double, Double)]()
          //var prevIterLowerBoundsMapNew =  Map[VertexId, Double]()   
          var newMap = Map[VertexId, NodeInfo]()
          var prevIterParentNodeLowerBoundsMapNew = Map[VertexId, (Double, Double)]() 
          var prevIterCurrentLowerBoundsMapNew = Map[VertexId, Double]()

         // if ((dstId1 == 162456) || (dstId1 == 120096) || (dstId1 == 51308))
         // {
         //    print ("444444444444444444444444409: starQueryGraphbfsTraverseWithBoundPruning a : " + "srcId: " + srcId1 +" " + srcId2 + " dstId: "+ dstId1 + " " + dstId2 + " nodeMapA: "+nodeMapA + " nodeMapB "+nodeMapB + " done \n")
         // }
            //  print ("410: starQueryGraphbfsTraverseWithBoundPruning updatedLowerBoundCloseScore : "+prevIterLowerBoundsMapA + "    " + prevIterLowerBoundsMapB + " ")
              
         // prevIterLowerBoundsMapA.keys.foreach{(specificNodeId) =>
         //combine and update only when there is update from any specificNode updated
           specificNodeIdLst.foreach{(specificNodeIdType: (VertexId, Int)) =>
            val specificNodeId = specificNodeIdType._1
            
            if (nodeMapA.contains(specificNodeId) && nodeMapB.contains(specificNodeId)) {
              
              //combine the new key->value pair into the nodeMapA；  update spDistance, hierarchicalLevelDistance, spNumber
              if (srcId1 != srcId2)             //need to judege here, because triplet.sendToDst may send mutliple time by different cpu cores or partitons
              {
                 val newSpDistance = math.min(nodeMapA(specificNodeId).spDistance, nodeMapA(specificNodeId).spDistance)               
                 val newHierLevelDistance = math.max(nodeMapA(specificNodeId).hierLevelDifference, nodeMapB(specificNodeId).hierLevelDifference)

                 //val newSpNumber = nodeMapB(specificNodeId).spNumber       //no change
                 val newSpNumber = nodeMapB(specificNodeId).spNumber +1       //+1
                
                 val tmpNodeInfo = nodeMapA(specificNodeId).copy(spDistance = newSpDistance, spNumber = newSpNumber, hierLevelDifference = newHierLevelDistance)
                 newMap += (specificNodeId -> tmpNodeInfo)
                 
                 //update prevIterParentNodeLowerBoundsMap
                //update prevIterCurrentLowerBoundsMap  the same as prevIterCurrentLowerBoundsMapA
                val newprevIterParentNodeLowerBound =  prevIterParentNodeLowerBoundsMapA(specificNodeId)._1 + prevIterParentNodeLowerBoundsMapB(specificNodeId)._1
                val newNeighborNodehierLevelDifference = math.max(prevIterParentNodeLowerBoundsMapA(specificNodeId)._2, prevIterParentNodeLowerBoundsMapB(specificNodeId)._2)
                prevIterParentNodeLowerBoundsMapNew += (specificNodeId -> (newprevIterParentNodeLowerBound, newNeighborNodehierLevelDifference))
                prevIterCurrentLowerBoundsMapNew  += (specificNodeId -> prevIterCurrentLowerBoundsMapA(specificNodeId))

               }
               else
               {
                  newMap += (specificNodeId -> nodeMapA(specificNodeId))
                  prevIterParentNodeLowerBoundsMapNew += (specificNodeId -> prevIterParentNodeLowerBoundsMapA(specificNodeId))
                  prevIterCurrentLowerBoundsMapNew  += (specificNodeId -> prevIterCurrentLowerBoundsMapA(specificNodeId))
               }
               
             
            }
            else if (nodeMapA.contains(specificNodeId)){        //only in b, put into A, because we want to return A
              newMap += (specificNodeId -> nodeMapA(specificNodeId))
              prevIterParentNodeLowerBoundsMapNew += (specificNodeId -> prevIterParentNodeLowerBoundsMapA(specificNodeId))
              prevIterCurrentLowerBoundsMapNew += (specificNodeId -> prevIterCurrentLowerBoundsMapA(specificNodeId))
              
            }
            else if (nodeMapB.contains(specificNodeId)){        //only in b, put into A, because we want to return A
              newMap += (specificNodeId -> nodeMapB(specificNodeId))
              prevIterParentNodeLowerBoundsMapNew += (specificNodeId -> prevIterParentNodeLowerBoundsMapB(specificNodeId))
              prevIterCurrentLowerBoundsMapNew += (specificNodeId -> prevIterCurrentLowerBoundsMapB(specificNodeId))
              
            }
            
          }
        // print ("504: starQueryGraphbfsTraverseWithBoundPruning: ", newMap)
        (nodeTypeId, newMap, prevIterParentNodeLowerBoundsMapNew, prevIterCurrentLowerBoundsMapNew, srcId1, dstId1)
        
        }
    ).cache()
      
    //no updated messages
    if (msgs.count == 0)
      (topKResultRdd, pathAnswerRdd)
      //  topKResultRdd                         //only return topKResultRdd
       
    g = g.ops.joinVertices(msgs) {
        (nodeId, oldAttr, newAttr) =>
        var nodeOldMap = oldAttr._2
        var nodeNewMap = newAttr._2
        val nodeTypeId = newAttr._1
        val prevIterParentNodeLowerBoundsMap = newAttr._3       //Map[VertexId, Double]()       neighbor node parent node
        val prevIterCurrentNodeLowerBoundsMap = newAttr._4              //Map[VertexId, Double]()          current node's previous iteration'

      //  var dstNodeTypeVisitFlag = true
        var newMap = nodeOldMap    // Map[VertexId, NodeInfo]()    //  //nodeOldMap         //Map[VertexId, NodeInfo]()           //initialization 
      //  prevIterLowerBoundsMap.keys.foreach{(specificNodeId) =>
        //  specificNodeIdLst.foreach{(specificNodeIdType: (VertexId, Int)) =>
        // val specificNodeId = specificNodeIdType._1
        var allNodesVisitedNumberNewTempFlag = true
        
         nodeNewMap.keys.foreach{(specificNodeId) =>
         //  if (nodeNewMap.contains(specificNodeId))
          // {
              if (nodeNewMap(specificNodeId).spDistance <= nodeOldMap(specificNodeId).spDistance)        //<  or <=
              {
               
                  // println("464 starQueryGraphbfsTraverse: "  + specificNodeId + " nodeId: " + nodeId)
                  val spDistance = nodeNewMap(specificNodeId).spDistance
                  val spNumber = nodeNewMap(specificNodeId).spNumber
                  val newhierLevelDifference =  nodeNewMap(specificNodeId).hierLevelDifference        //(-1) x hierLevelDifference； downward inheritance

                  //update closeness score from this sepcific nodeId
                  val parentNodeHierarchicalLevel = newhierLevelDifference - prevIterParentNodeLowerBoundsMap(specificNodeId)._2         //parent node max hierarchical level difference
                  val newClosenessScore = calculateClosenessScore(spDistance, spNumber, parentNodeHierarchicalLevel, newhierLevelDifference)      //node similarity score

                  //update lower bound score from this specific nodeId
                  // val updatedLowerBoundCloseScore = calculateLowerBound(specificNodeId, nodeNewMap, prevIterLowerBoundsMapA(specificNodeId))
                  val prevIterCurrentNodeLowerScore = prevIterCurrentNodeLowerBoundsMap(specificNodeId)
                  val parentNodePrevIterLowerScore = prevIterParentNodeLowerBoundsMap(specificNodeId)._1
                  val neighborNodehierLevelDifference = prevIterParentNodeLowerBoundsMap(specificNodeId)._2
                 // val parentNodeHierarchicalLeve = newhierLevelDifference - prevIterParentNodeLowerBoundsMap(specificNodeId)._2

                  val newLowerBoundCScore = calculateNodeSimilarityLowerBound(spDistance, parentNodeHierarchicalLevel, prevIterCurrentNodeLowerScore,  parentNodePrevIterLowerScore, neighborNodehierLevelDifference)

                  val newUpperBoundCScore = calculateNodeSimilarityUpperBound(newLowerBoundCScore, spDistance, newhierLevelDifference)

                 // val newLowerBoundCScore =  nodeNewMap(specificNodeId).lowerBoundCloseScore // math.min(scala.math.pow(ALPHA, (spDistance-newhierLevelDifference-1)), nodeNewMap(specificNodeId).lowerBoundCloseScore)           // error ??
                 // val newUpperBoundCScore = nodeNewMap(specificNodeId).upperBoundCloseScore // calculateUpperBound(newLowerBoundCScore, spDistance, newhierLevelDifference)
                 //test
                 // if (nodeId == 3)
                 //   {
                 //     println("464 starQueryGraphbfsTraverse: "  + specificNodeId + " nodeId: " + nodeId +  " prevIterCurrentNodeLowerScore: " + prevIterCurrentNodeLowerScore +" parentNodeScore: " + parentNodePrevIterLowerScore + " sd: " +nodeNewMap(specificNodeId).spDistance + " hierLevel: " + newhierLevelDifference + " spNum: " + spNumber)
                 //     println("467 starQueryGraphbfsTraverse: " + newLowerBoundCScore + " _ " + newUpperBoundCScore+ " score: " + newClosenessScore + "  currentSatisfiedNodesNumber: " +currentSatisfiedNodesNumber)
                 //   }

                  val tmpNodeInfo = nodeNewMap(specificNodeId).copy(visitedColor = GREY.id, spNumber = spNumber, hierLevelDifference = newhierLevelDifference, closenessNodeScore = newClosenessScore,
                                                                     lowerBoundCloseScore = newLowerBoundCScore, upperBoundCloseScore = newUpperBoundCScore)  //update closenessNodeScore  etc
                  newMap += (specificNodeId -> tmpNodeInfo)
              }         
      }
          
          //test println
         // if (nodeId == 40)
       // (nodeTypeId, newMap)
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
     
     // print ("555: starQueryGraphbfsTraverseWithBoundPruning  g.vertices.isEmpty: ", g.vertices.isEmpty())
     // val x1 = 1
     // val x2 = 2
     // print ("556: starQueryGraphbfsTraverseWithBoundPruning  g.vertices.count: ", g.vertices.count())


     //find all nodes that have at least visited from one specific node
     val allNodesVisitedParts =  g.vertices.filter{ case x=>
        val nodeMap = x._2._2
        
        //judge the nodes is visited from all the specific nodes
        def getAllVisiteFlag(nodeMap: Map[VertexId, NodeInfo]) ={             //define function
          var visitedFlag = false
          for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
              if (nodeInfo.spDistance != Long.MaxValue)        //any one exists
                visitedFlag = true
          }
          visitedFlag
        }
        getAllVisiteFlag(nodeMap)
      }
      
     // val elapsedTime = System.currentTimeMillis() - startTime  
     // print("iterationCount " + iterationCount + " " + "runtime: "+ elapsedTime + " ms\n" )

      val currentIterateNodeResult = allNodesVisitedParts.filter{ case x => 
        
        val nodeMap = x._2._2
         def getAllVisiteFlag(nodeMap: Map[VertexId, NodeInfo]) ={             //define function
          var visitedFlag = true
          for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
           if (nodeInfo.spDistance == Long.MaxValue)
             visitedFlag = false
          }
          visitedFlag
        }
        (getAllVisiteFlag(nodeMap)) && (x._2._1 == dstTypeId)
        
      }
        
      // currentIterateNodeResult.take(10).foreach(println)
      
      //how many satisfied nodes into top K nodes list
      currentSatisfiedNodesNumber = currentIterateNodeResult.count()
     // print ("649: starQueryGraphbfsTraverseWithBoundPruning currentIterateNodeResult: ", allNodesVisitedNumber, currentSatisfiedNodesNumber)
      
      
     g = setnodeIdColorForBound(allNodesVisitedParts, g)                 //update bounding nodes color
      
      oldAllNodesVisitedNumber = allNodesVisitedNumber
      //print ("604: starQueryGraphbfsTraverseWithBoundPruning twoPreviousOldAllNodesVisitedNumber oldAllNodesVisitedNumber: ", allNodesVisitedNumber, twoPreviousOldAllNodesVisitedNumber, oldAllNodesVisitedNumber)
      
      // print ("560: starQueryGraphbfsTraverseWithBoundPruning test: ", allNodesVisited.take(1).foreach(println))
      
      //how many nodes have been visited from all specific nodes
      allNodesVisitedNumber =  allNodesVisitedParts.count()
     // print ("562: starQueryGraphbfsTraverseWithBoundPruning currentIterateNodeResult: ",iterationCount, allNodesVisitedNumber, oldAllNodesVisitedNumber, currentSatisfiedNodesNumber, topKKthLowerBoundScore, " \n")
      
      //twoPreviousOldAllNodesVisitedNumber = oldAllNodesVisitedNumber
      //statisitics of iteration number
      iterationCount += 1
          
      //put result into topK final list top K
      //var transferedResultRdd: RDD[(VertexId, Int)] = sc.emptyRDD()
      //whether it has topK candidates satisfied.
      if (currentSatisfiedNodesNumber <= TOPK)
      {
          topKResultRdd =  currentIterateNodeResult.map(x=>
          (x._1, (calculateNodeScoreStarquery(x._2._2), calculateMatchingScoreLowerBound(x._2._2),  calculateMatchingScoreUpperBound(x._2._2), x._2._1.toString.toInt, x._2._2)))
        
          //print ("584: starQueryGraphbfsTraverseWithBoundPruning topKResultRdd： "+ TOPK+ "--" + topKResultRdd.count)
          
      }
      else{              //there are k element in the list
        //get topK union by calculateNodesScoreStarquery
          val topKResultRddArray =  currentIterateNodeResult.map(x=>
          (x._1, (calculateNodeScoreStarquery(x._2._2), calculateMatchingScoreLowerBound(x._2._2),  calculateMatchingScoreUpperBound(x._2._2), x._2._1.toString.toInt, x._2._2))).takeOrdered(TOPK)(Ordering[Double].reverse.on(x=>x._2._1))    //sort by matching score calculateNodeScoreStarquery result
       
          //get the kth smallest lower bound score in the topKResultRddArray
          topKKthLowerBoundScore = topKResultRddArray.sortBy(x=>x._2._2).head._2._2       //sorted by lower bound matching score
           
          topKResultRdd = sc.parallelize(topKResultRddArray)                //transfer to RDD data structure
         // print ("598: starQueryGraphbfsTraverseWithBoundPruning topKResultRdd: "+ TOPK + "----" + topKResultRdd.count, topKKthLowerBoundScore)
        //  
        // pathAnswerRdd = getPathforAnswers(sc, topKResultRdd, g)
        //  topKResultRdd
        //(topKResultRdd, pathAnswerRdd)
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
  
  //judge src and dst Id is in the pair
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
  def starQueryExeute[VD, ED](sc: SparkContext, graph: Graph[VD, ED], specificNodeIdLst: List[(VertexId,Int)], dstTypeId: Int, databaseType: Int, inputFileNodeInfoPath: String, outputFilePath: String, runTimeoutputFilePath: String, hierarchialRelation: Boolean) = {
    
   // val newGraph = preProcessGraphDeleteEdge(graph, List((40, 58)))      //preprocess for different query
    
    //val topKResultRdd = starQueryGraphbfsTraverseWithBoundPruning(sc, graph, specificNodeIdLst, dstTypeId, databaseType, runTimeoutputFilePath, hierarchialRelation)
    
    val answers = starQueryGraphbfsTraverseWithBoundPruning(sc, graph, specificNodeIdLst, dstTypeId, databaseType, runTimeoutputFilePath, hierarchialRelation)
    
    val topKResultRdd = answers._1
    val pathAnswerRdd = answers._2

    //topKResultRdd.count()
    val nodeInfoRdd =  graphInputCommon.readNodeInfoName(sc, inputFileNodeInfoPath)
    
    val resultStarQueryRdd = nodeInfoRdd.join(topKResultRdd)    //join (k,v), (k, w) => k, (v, w)
    //print ("385: starQueryExeute resultStarQueryRdd: \n")
    
    //resultStarQueryRdd.collect().foreach(println)
    //resultStarQueryRdd.coalesce(1).saveAsTextFile(outputFileNode)        //coalesce into 1 file, it is small data output
    print ("422: starQueryExeute pathAnswerRdd: "+ topKResultRdd.count() + "\n")
    
    if (!pathAnswerRdd.isEmpty())
    {
        pathAnswerRdd.collect().foreach(println)

    }
    
    // outputFilePath
    if (outputFilePath != null)
    {
      resultStarQueryRdd.coalesce(1).saveAsTextFile(outputFilePath)        //coalesce into 1 file, it is small data output
    }
    // topKResultRdd.coalesce(1).saveAsTextFile(outputFilePath)        //coalesce into 1 file, it is small data output

  }

  }


