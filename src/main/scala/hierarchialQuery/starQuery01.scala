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


object starQuery01 {
 
  var TOPK = 0                 //top K candidate answer, set by main function parameter
  val ALPHA = 0.9               //propagation factor
  val N = 1.05
  val numTasks = 8                   //how many task for one core can execute in parallell

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
      
      println("bfs dstAttr: ", dstAttr)
     // g.vertices.take(5).foreach(println)

    }

    // Traverse backward from dst and collect the path
    var path: List[VertexId] = dstAttr._2 :: dst :: Nil
    println("bfs 50 initial Path: ", path)
    
    while (path.head != src) {
      path = g.vertices.filter(_._1 == path.head).first()._2._2 :: path
    }

     println("sssp path:  ", path)

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
    println("77 singleSourceGraphbfsTraverse :  ")

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
           println("108 singleSourceGraphbfsTraverse: ", triplet.srcAttr._2)
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
  def getPathforAnswers[VD, ED](sc: SparkContext, topkResultRdd: RDD[(VertexId, (Double, Map[VertexId, NodeInfo]))], graphExtended: Graph[(VD, Map[VertexId, NodeInfo]), ED] ) = {
    var pathAnswersLst = new ListBuffer[(VertexId, Map[VertexId, ListBuffer[VertexId]])]()
    val topkResultList = topkResultRdd.collect().toList
    //print ("227 getPathforAnswers ttttttttt: " + topkResultList)
    
    for (lstTuple <- topkResultList){
      val nodeId = lstTuple._1
      val nodeMap = lstTuple._2._2
      
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
                val prevNodeId = graph.vertices.filter(_._1 == tmpPathLstBuffer.head).first()._2._2(specific).parentId
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
  //println("270 starQueryGraphbfsTraverse :" + pathAnswersLst +"\n")
  pathAnswersRDD
  
}
  
/*
//star query traverse
def starQueryGraphbfsTraverseNoBounds[VD, ED](sc: SparkContext, graph: Graph[VD, ED], specificNodeIdLst: List[(VertexId, Int)], dstTypeId: Int) = {
     
    val startTime = System.currentTimeMillis()              //System.nanoTime()

    //Vertex's  property is (VD, Map[VertexId, NodeInfo]) ; VD is the nodeId property-- nodeIdType here; Map's key: specificNodeId, value is NodeInfo
    var g: Graph[(VD, Map[VertexId, NodeInfo]), ED] =
      graph.mapVertices((id, nodeIdType) => (nodeIdType, 
                                             specificNodeIdLst.map(specificNodeIdType=> specificNodeIdType._1-> NodeInfo(specificNodeIdType._1,
                                                                   specificNodeIdType._2, if (id == specificNodeIdType._1) 0 else Long.MaxValue, if (id == specificNodeIdType._1) 1 else 0, 0, 0.0, 0, if (id == specificNodeIdType._1) GREY.id else WHITE.id )).toMap
      )).cache()
                                 
    // g.vertices.take(5).foreach(println)
    println("286 starQueryGraphbfsTraverse :  \n")
    //iterations for bfs begin
    var iterationCount = 0
    var currentSatisfiedNodesNumber: Long = 0L
    var allNodesVisitedNumber: Long = 0L
    var topKResultRdd: RDD[(VertexId, (Double, Map[VertexId, NodeInfo]))] = sc.emptyRDD[(VertexId, (Double, Map[VertexId, NodeInfo]))]           //Return Result RDD 

    while (currentSatisfiedNodesNumber < TOPK && allNodesVisitedNumber < graph.ops.numVertices)        // find top k or whole graph iteration end
    {
      //println("208 starQueryGraphbfsTraverse iterationCount: ", iterationCount)

      val msgs: VertexRDD[(VD, Map[VertexId, NodeInfo])] = g.aggregateMessages[(VD, Map[VertexId, NodeInfo])](
        triplet => {
          val srcNodeMap = triplet.srcAttr._2
          //println("266 starQueryGraphbfsTraverse srcNodeMap: ", srcNodeMap)
          var dstNodeMap = triplet.dstAttr._2
          var newdstNodeMap = dstNodeMap
          //println("273 starQueryGraphbfsTraverse newdstNodeMap: "+  triplet.srcAttr._1.toString.toInt+ "   " + dstTypeId)
          specificNodeIdLst.foreach((specificNodeIdType: (VertexId, Int)) => 
            //val sourceIdType = triplet.srcAttr._1
            if (srcNodeMap(specificNodeIdType._1).spDistance != Long.MaxValue && srcNodeMap(specificNodeIdType._1).spDistance + 1  < dstNodeMap(specificNodeIdType._1).spDistance)
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
                  val currentNodeTypeId = triplet.dstAttr._1 
                  //if (specificNodeIdType == 1)
                  //    println("295 starQueryGraphbfsTraverse newdstNodeMap: ", specificNodeIdType, dstTypeId)
              }
              val currentNodeTypeId = triplet.dstAttr._1 
              triplet.sendToDst((currentNodeTypeId, newdstNodeMap))
                          
            }
          )
        },
        
        (a, b) => {
          val nodeTypeId = a._1
          val nodeMapA = a._2
          val nodeMapB = b._2
          var newMap = Map[VertexId, NodeInfo]()
          specificNodeIdLst.foreach((specificNodeIdType: (VertexId, Int)) =>

           //keep current specificNodeId's map value
          if (nodeMapA(specificNodeIdType._1).spDistance < nodeMapB(specificNodeIdType._1).spDistance){  
              //update visit color
              val tmpNodeInfo = nodeMapA(specificNodeIdType._1).copy(vistiedColor = GREY.id)  //update spNumber

              newMap += (specificNodeIdType._1 -> tmpNodeInfo)       //key -> value
              
          }
          else if (nodeMapA(specificNodeIdType._1).spDistance == nodeMapB(specificNodeIdType._1).spDistance){   
             val tmpNodeInfo = nodeMapA(specificNodeIdType._1).copy(spNumber = nodeMapA(specificNodeIdType._1).spNumber+1, vistiedColor = GREY.id)  //update spNumber
              newMap += (specificNodeIdType._1 -> tmpNodeInfo)
          }
          else{
              val tmpNodeInfo = nodeMapB(specificNodeIdType._1).copy(vistiedColor = GREY.id)  //update spNumber
              newMap += (specificNodeIdType._1 -> tmpNodeInfo)
          })
         //print ("286: starQueryGraphbfsTraverse: ", newMap)
         (nodeTypeId, newMap)
          
        }
    ).cache()
      
    //no updated message 
    if (msgs.count == 0)
      topKResultRdd
    
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
              val tmpNodeInfo = nodeNewMap(specificNodeIdType._1).copy(closenessNodeScore = newClosenessScore, hierLevelDifference = newhierLevelDifference)  //update closenessNodeScore 
              newMap += (specificNodeIdType._1 -> tmpNodeInfo)
          }
          else
          {
              newMap += (specificNodeIdType._1 -> nodeOldMap(specificNodeIdType._1))
          }
           
          if (newMap(specificNodeIdType._1).vistiedColor != GREY.id)
           {
             dstNodeTypeVisitFlag = false
           }
           
        }
          
           
          //test println
         // if (nodeId == 40)
         (nodeTypeId, newMap)

      }.cache()
      
      //check all the nodes have been updated, i.e. visited 
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
      
      //how many nodes have been visited from all specific nodes
      allNodesVisitedNumber =  allNodesVisited.count()
      val currentIterateNodeResult = allNodesVisited.filter(x => x._2._1 == dstTypeId)
      
     // currentIterateNodeResult.take(10).foreach(println)
      //how many satisfied nodes into top K nodes list
      currentSatisfiedNodesNumber = currentIterateNodeResult.count()
      print ("336: starQueryGraphbfsTraverse currentIterateNodeResult: ", allNodesVisitedNumber, currentSatisfiedNodesNumber)
      
      //statisitics of iteration number
      iterationCount = iterationCount + 1

      //put result into topK final list top K
      //var transferedResultRdd: RDD[(VertexId, Int)] = sc.emptyRDD()
      //whether it has topK candidates satisfied.
      if (currentSatisfiedNodesNumber <= TOPK)
      {
          topKResultRdd =  currentIterateNodeResult.map(x=>
          (x._1, (calculateNodeScoreStarquery(x._2._2), x._2._2)))
        
          print ("352: starQueryGraphbfsTraverse topKResultRdd \n"+ TOPK)
          topKResultRdd.collect().foreach(println)
          
         // topKResultRdd
      }
      else{
        //get topK union by calculateNodesScoreStarquery
          val topKResultRddArray =  currentIterateNodeResult.map(x=>
          (x._1, (calculateNodeScoreStarquery(x._2._2), x._2._2))).takeOrdered(TOPK)(Ordering[Double].reverse.on(x=>x._2._1))    //sort by calculateNodeScoreStarquery result
        
          topKResultRdd = sc.parallelize(topKResultRddArray)                //transfer to RDD data structure
          print ("362: starQueryGraphbfsTraverse topKResultRdd: "+ TOPK + " " + topKResultRddArray)
          topKResultRdd.collect().foreach(println)
        //  topKResultRdd
      } 
      

    }
    
    val endTime = System.currentTimeMillis()   
    println("379 starQueryGraphbfsTraverse runtime: "+ (endTime-startTime) + " ms")
    
    //get shortest path for the top K answer:
    val pathAnswerRdd = getPathforAnswers(sc, topKResultRdd, g)
    
    (topKResultRdd, pathAnswerRdd)
    //topKResultRdd                         //only return topKResultRdd
    
  }
*/
   
//star query traverse with pruning
// starQueryGraphbfsTraverseWithBoundPruning

def starQueryGraphbfsTraverseWithBoundPruning[VD, ED](sc: SparkContext, graph: Graph[VD, ED], specificNodeIdLst: List[(VertexId, Int)], dstTypeId: Int) = {
     
    val startTime = System.currentTimeMillis()              //System.nanoTime()

    //Vertex's  property is (VD, Map[VertexId, NodeInfo]) ; VD is the nodeId property-- nodeIdType here; Map's key: specificNodeId, value is NodeInfo
    var g: Graph[(VD, Map[VertexId, NodeInfo]), ED] =
      graph.mapVertices((id, nodeIdType) => (nodeIdType, 
                                             specificNodeIdLst.map(specificNodeIdType=> specificNodeIdType._1-> NodeInfo(specificNodeIdType._1,
                                                                                                                         specificNodeIdType._2, if (id == specificNodeIdType._1) 0 else Long.MaxValue, if (id == specificNodeIdType._1) 1 else 0, 0, 0.0, 0, if (id == specificNodeIdType._1) GREY.id else WHITE.id,
                                                                                                                         if (id == specificNodeIdType._1) 1.0 else 0.0,  if (id == specificNodeIdType._1) 1 else {N*scala.math.pow(ALPHA, 1)})).toMap
      )).cache()
                                 
    // g.vertices.take(5).foreach(println)
    //println("486 starQueryGraphbfsTraverseWithBoundPruning :  \n")
    //iterations for bfs begin
    var iterationCount = 0
    var currentSatisfiedNodesNumber: Long = 0L
    var allNodesVisitedNumber: Long = 0L
    var oldAllNodesVisitedNumber: Long = -1L 
    var twoPreviousOldAllNodesVisitedNumber: Long = -2L 
    
    var topKResultRdd: RDD[(VertexId, (Double, Map[VertexId, NodeInfo]))] = sc.emptyRDD[(VertexId, (Double, Map[VertexId, NodeInfo]))]           //Return Result RDD 

    
    val dstNodesNumberGraph = graph.vertices.filter(x=>x._2 == dstTypeId)
    println("504 starQueryGraphbfsTraverseWithBoundPruning :  ", dstNodesNumberGraph.count)
   
  //no value change or all the destination node has been visited
  while (twoPreviousOldAllNodesVisitedNumber != oldAllNodesVisitedNumber && currentSatisfiedNodesNumber < dstNodesNumberGraph.count && allNodesVisitedNumber < graph.ops.numVertices) //currentSatisfiedNodesNumber < TOPK &&; find top k or whole graph iteration end    {
      //println("208 starQueryGraphbfsTraverse iterationCount: ", iterationCount)
  {
      val msgs: VertexRDD[(VD, Map[VertexId, NodeInfo])] = g.aggregateMessages[(VD, Map[VertexId, NodeInfo])](
        triplet => {
          val srcNodeMap = triplet.srcAttr._2
          //println("266 starQueryGraphbfsTraverse srcNodeMap: ", srcNodeMap)
          var dstNodeMap = triplet.dstAttr._2
          var newdstNodeMap = dstNodeMap
          //println("273 starQueryGraphbfsTraverse newdstNodeMap: "+  triplet.srcAttr._1.toString.toInt+ "   " + dstTypeId)
          specificNodeIdLst.foreach((specificNodeIdType: (VertexId, Int)) => 
            //val sourceIdType = triplet.srcAttr._1
            if (srcNodeMap(specificNodeIdType._1).spDistance != Long.MaxValue && srcNodeMap(specificNodeIdType._1).spDistance + 1  < dstNodeMap(specificNodeIdType._1).spDistance)
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
                  val currentNodeTypeId = triplet.dstAttr._1 
                  //if (specificNodeIdType == 1)
                  //    println("295 starQueryGraphbfsTraverseWithBoundPruning newdstNodeMap: ", specificNodeIdType, dstTypeId)
              }
              val currentNodeTypeId = triplet.dstAttr._1 
              triplet.sendToDst((currentNodeTypeId, newdstNodeMap))
                          
            }
          )
        },
        
        (a, b) => {
          val nodeTypeId = a._1
          val nodeMapA = a._2
          val nodeMapB = b._2
          var newMap = Map[VertexId, NodeInfo]()
          specificNodeIdLst.foreach((specificNodeIdType: (VertexId, Int)) =>

           //keep current specificNodeId's map value
          if (nodeMapA(specificNodeIdType._1).spDistance < nodeMapB(specificNodeIdType._1).spDistance){  
              //update visit color
              val tmpNodeInfo = nodeMapA(specificNodeIdType._1).copy(visitedColor = GREY.id)  //update spNumber

              newMap += (specificNodeIdType._1 -> tmpNodeInfo)       //key -> value
              
          }
          else if (nodeMapA(specificNodeIdType._1).spDistance == nodeMapB(specificNodeIdType._1).spDistance){   
             val tmpNodeInfo = nodeMapA(specificNodeIdType._1).copy(spNumber = nodeMapA(specificNodeIdType._1).spNumber+1, visitedColor = GREY.id)  //update spNumber
              newMap += (specificNodeIdType._1 -> tmpNodeInfo)
          }
          else{
              val tmpNodeInfo = nodeMapB(specificNodeIdType._1).copy(visitedColor = GREY.id)  //update spNumber
              newMap += (specificNodeIdType._1 -> tmpNodeInfo)
          })
         //print ("286: starQueryGraphbfsTraverseWithBoundPruning: ", newMap)
         (nodeTypeId, newMap)
          
        }
    ).cache()
      
    //no updated message 
    if (msgs.count == 0)
      topKResultRdd
    
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
              val tmpNodeInfo = nodeNewMap(specificNodeIdType._1).copy(closenessNodeScore = newClosenessScore, hierLevelDifference = newhierLevelDifference)  //update closenessNodeScore 
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
      
      //check all the nodes have been updated, i.e. visited 
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
        
      //how many nodes have been visited from all specific nodes
      allNodesVisitedNumber =  allNodesVisited.count()
      
      print ("644: starQueryGraphbfsTraverseWithBoundPruning currentIterateNodeResult: ", twoPreviousOldAllNodesVisitedNumber, oldAllNodesVisitedNumber)
      
      twoPreviousOldAllNodesVisitedNumber = oldAllNodesVisitedNumber
      
      val currentIterateNodeResult = allNodesVisited.filter(x => x._2._1 == dstTypeId)
      // currentIterateNodeResult.take(10).foreach(println)
      
      //how many satisfied nodes into top K nodes list
      currentSatisfiedNodesNumber = currentIterateNodeResult.count()
      print ("649: starQueryGraphbfsTraverseWithBoundPruning currentIterateNodeResult: ", allNodesVisitedNumber, currentSatisfiedNodesNumber)
      
      //statisitics of iteration number
      iterationCount += 1

      //put result into topK final list top K
      //var transferedResultRdd: RDD[(VertexId, Int)] = sc.emptyRDD()
      //whether it has topK candidates satisfied.
      if (currentSatisfiedNodesNumber <= TOPK)
      {
          topKResultRdd =  currentIterateNodeResult.map(x=>
          (x._1, (calculateNodeScoreStarquery(x._2._2), x._2._2)))
        
          print ("653: starQueryGraphbfsTraverseWithBoundPruning topKResultRdd： "+ TOPK+ "--" + topKResultRdd.count)
          //topKResultRdd.collect().foreach(println)
          
         // topKResultRdd
      }
      else{
        //get topK union by calculateNodesScoreStarquery
          val topKResultRddArray =  currentIterateNodeResult.map(x=>
          (x._1, (calculateNodeScoreStarquery(x._2._2), x._2._2))).takeOrdered(TOPK)(Ordering[Double].reverse.on(x=>x._2._1))    //sort by calculateNodeScoreStarquery result
        
          topKResultRdd = sc.parallelize(topKResultRddArray)                //transfer to RDD data structure
         // print ("664: starQueryGraphbfsTraverseWithBoundPruning topKResultRdd: "+ TOPK + "----" + topKResultRdd.count)
         // topKResultRdd.collect().foreach(println)
        //  topKResultRdd
      } 
      
      oldAllNodesVisitedNumber = allNodesVisitedNumber
      
    }
    
    val endTime = System.currentTimeMillis()   
    println("673 starQueryGraphbfsTraverseWithBoundPruning runtime: "+ (endTime-startTime) + " ms")
    
    //get shortest path for the top K answer:
    val pathAnswerRdd = getPathforAnswers(sc, topKResultRdd, g)
    
    (topKResultRdd, pathAnswerRdd)
    //topKResultRdd                         //only return topKResultRdd
  }
  
  
  
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
  def starQueryExeute[VD, ED](sc: SparkContext, graph: Graph[VD, ED], specificNodeIdLst: List[(VertexId,Int)], dstTypeId: Int, inputFileNodeInfoPath: String, outputFileNode: String, outputFilePath: String) = {
    
   // val newGraph = preProcessGraphDeleteEdge(graph, List((40, 58)))      //preprocess for different query
    
    //val answers = starQueryGraphbfsTraverseNoBounds(sc, graph, specificNodeIdLst, dstTypeId)
    val answers = starQueryGraphbfsTraverseWithBoundPruning(sc, graph, specificNodeIdLst, dstTypeId)
    val topKResultRdd = answers._1
    val pathAnswerRdd = answers._2

    //topKResultRdd.count()
    val nodeInfoRdd =  graphInputCommon.readNodeInfoName(sc, inputFileNodeInfoPath)
    //join (k,v), (k, w) => k, (v, w)
    val resultStarQueryRdd = nodeInfoRdd.join(topKResultRdd)
    print ("385: starQueryExeute resultStarQueryRdd: \n")
    
    resultStarQueryRdd.collect().foreach(println)
    resultStarQueryRdd.coalesce(1).saveAsTextFile(outputFileNode)        //coalesce into 1 file, it is small data output
   
    print ("422: starQueryExeute pathAnswerRdd: \n")
    pathAnswerRdd.collect().foreach(println)
    // outputFilePath
    resultStarQueryRdd.coalesce(1).saveAsTextFile(outputFilePath)        //coalesce into 1 file, it is small data output

    
  }
  
   
}


