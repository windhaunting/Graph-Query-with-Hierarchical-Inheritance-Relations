/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.scala.hierarchialQuery

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

import collection.immutable.{Map,HashMap}
import scala.collection.mutable.ListBuffer

import main.scala.hierarchialQuery.nodeTypeProductEnum._
import main.scala.hierarchialQuery.visitedColorEnum._

import java.io._

//non star query nodes;  query two or more unknown nodes
//first star query; then use branch and bound to prune to to get topK result
object nonStarQuery {
    
  var nonStarQuery_TOPK = 0
  
  //get every node's matching score from step 1's star query result
  def getStarQueryCandidateScoreHashMap(topKStarRstLstBuffer: ListBuffer[RDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))]]) = {
    
    var i = 0
    var starQueryNodeHashMap = Map[(Int, VertexId), Double]()    //(node index (from where topKStarRstLstBuffer indexes), matchingScore
    while (i < topKStarRstLstBuffer.length)
    {
      val starQueryNodeRstLst = topKStarRstLstBuffer(i).map{x => (x._1, x._2._1)}.collect().toList      // => (nodeId, node matchingScore)
      
      for (starQueryNodeIdRst <- starQueryNodeRstLst)
      {
          starQueryNodeHashMap += ((i, starQueryNodeIdRst._1) ->  starQueryNodeIdRst._2)
      }
      i = i + 1
    }
    starQueryNodeHashMap 
  }
  
   /*                              
  //non Star query with two unknown nodes
  //input: previous star query result
  //output: final unknown query tuples, here it is 2
  def nonStarQueryGraphbfsTraverseTwoQueryNodes[VD, ED](sc: SparkContext, graph: Graph[VD, ED], topKStarRstLstBuffer: ListBuffer[RDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))]], dstTypeIdLst: List[Int]) = {
    
    val startTime = System.currentTimeMillis()              //System.nanoTime()

    val starQueryNodeHashMap = getStarQueryCandidateScoreHashMap(topKStarRstLstBuffer)
    
    var i = 0
    
    var topKNonStarResultRdd: RDD[(List[VertexId], Double)] = sc.emptyRDD[(List[VertexId], Double)]          //Return Result RDD, (nodeId, matchingScore, lowerBound, upperBound, nodeMap)

    while (i < topKStarRstLstBuffer.length)      
    {
       //initialize the source list, as the new specificNodeIdLst
        val specificNodeIdLst = topKStarRstLstBuffer(i).map{x => (x._1, x._2._4, x._2._1)}         // => (nodeId, nodeTypeId, node matchingScore)
         .collect().toList.sortBy(x=>x._3)            // sorted by node matching score

        //val specificNodeIdLstHashMap = specificNodeIdLst.map{s => (s._1, (s._2, s._3))}

        //print ("nonStarQueryGraphbfsTraverseTwoQueryNodes sourceLst: " + specificNodeIdLst + " " + specificNodeIdLstHashMap + "\n")  

        //check the visited nodes have the another unkown  destination candidates nodes; that is in the topKStarRstLst(i+1) 
        val nextUnknownDestNodeIdLst = topKStarRstLstBuffer(i+1).map{x => (x._1, x._2._4, x._2._1)}         // => (nodeId, nodeTypeId, node matchingScore)
         .collect().toList.sortBy(x=>x._3)            // sorted by node matching score
         
      
       //initialize from first topKStarQuery as specificNodeList
       var g: Graph[(VD, Map[VertexId, NodeInfo]), ED] =
       graph.mapVertices((id, nodeIdType) => (nodeIdType, 
                                              specificNodeIdLst.map(specificNodeIdType=> specificNodeIdType._1-> NodeInfo(specificNodeIdType._1, 
                                                                                                                          specificNodeIdType._2, if (id == specificNodeIdType._1) 0 else Long.MaxValue, if (id == specificNodeIdType._1) 1 else 0, 0, 0.0, 0, if (id == specificNodeIdType._1) GREY.id else WHITE.id, if (id == specificNodeIdType._1) 1.0 else 0.0,  if (id == specificNodeIdType._1) 1 else {starQuery.N*scala.math.pow(starQuery.ALPHA, 1)})).toMap
       )).cache()

       val dstTypeId = dstTypeIdLst(i+1)              //updated the dstTypeId

       //var topKResultRdd: RDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))] = sc.emptyRDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))]           //Return Result RDD, (nodeId, matchingScore, lowerBound, upperBound, nodeMap)
       

      var allNodesVisitedNumber: Long = 0L                         // all nodes visited
      var oldAllNodesVisitedNumber: Long = -1L                     //previous iteration nodes visited
      var twoPreviousOldAllNodesVisitedNumber: Long = -2L          //previous and previous iteration nodes visited
    
      var allNodesVisitedAllSpecificsNumber = 0L
       //aggregate the message 
       while (nextUnknownDestNodeIdLst.size != allNodesVisitedAllSpecificsNumber && twoPreviousOldAllNodesVisitedNumber != oldAllNodesVisitedNumber && allNodesVisitedNumber < graph.ops.numVertices)
       {
         val msgs: VertexRDD[(VD, Map[VertexId, NodeInfo])] = g.aggregateMessages[(VD, Map[VertexId, NodeInfo])](
         triplet => {
           val srcNodeMap = triplet.srcAttr._2
           //println("266 starQueryGraphbfsTraverse srcNodeMap: ", srcNodeMap)
           var dstNodeMap = triplet.dstAttr._2
           var newdstNodeMap = dstNodeMap

           specificNodeIdLst.foreach((specificNodeIdType: (VertexId, Int, Double)) => 
               if (srcNodeMap(specificNodeIdType._1).spDistance != Long.MaxValue && srcNodeMap(specificNodeIdType._1).spDistance + 1  < dstNodeMap(specificNodeIdType._1).spDistance)
               {
                   val specificNodeId = specificNodeIdType._1
                   //val specNodeIdType = specificNodeIdType._2  

                  //update spDist and parentId only
                   val tmpNodeInfo = srcNodeMap(specificNodeId).copy(spDistance = srcNodeMap(specificNodeId).spDistance+1, parentId = triplet.srcId)  
                   //update dstNodeMap 
                   newdstNodeMap += (specificNodeId -> tmpNodeInfo)              

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
            specificNodeIdLst.foreach((specificNodeIdType: (VertexId, Int, Double)) =>
               //keep current specificNodeId's map value
             if (nodeMapA(specificNodeIdType._1).spDistance < nodeMapB(specificNodeIdType._1).spDistance){  
               //update visit color,  lowerBoundCloseness Score
               val updatedLowerBoundCloseScore = 0      //calculateLowerBound(specificNodeIdType._1, nodeMapA)

               val tmpNodeInfo = nodeMapA(specificNodeIdType._1).copy(visitedColor = GREY.id, lowerBoundCloseScore = updatedLowerBoundCloseScore)  //update color visited
               newMap += (specificNodeIdType._1 -> tmpNodeInfo)       //update key -> value
               //key -> value
             }
             else if (nodeMapA(specificNodeIdType._1).spDistance == nodeMapB(specificNodeIdType._1).spDistance){   
               //update bound
               val updatedLowerBoundCloseScoreA = 0  // calculateLowerBound(specificNodeIdType._1, nodeMapA)
               val updatedLowerBoundCloseScoreB = 0      //calculateLowerBound(specificNodeIdType._1, nodeMapB)

               val updatedLowerBoundCloseScore =  updatedLowerBoundCloseScoreA + updatedLowerBoundCloseScoreB
               val tmpNodeInfo = nodeMapA(specificNodeIdType._1).copy(spNumber = nodeMapA(specificNodeIdType._1).spNumber+1, visitedColor = GREY.id, 
                                                                      lowerBoundCloseScore = updatedLowerBoundCloseScore)  //update spNumber
               newMap += (specificNodeIdType._1 -> tmpNodeInfo)

           }
           else
             {
               //nodeMapB(specificNodeIdType._1).lowerBoundCloseScore + ....
               val updatedLowerBoundCloseScore = 0  // calculateLowerBound(specificNodeIdType._1, nodeMapB)

               val tmpNodeInfo = nodeMapB(specificNodeIdType._1).copy(visitedColor = GREY.id, lowerBoundCloseScore = updatedLowerBoundCloseScore)  //update color visited
               newMap += (specificNodeIdType._1 -> tmpNodeInfo)
             }
           )

          (nodeTypeId, newMap)

         }                
       ).cache()

       //no updated messages
      //if (msgs.count == 0)
       //(topKResultRdd, pathAnswerRdd)
       // topKResultRdd                         //only return topKResultRdd

      g = g.ops.joinVertices(msgs) {
       (nodeId, oldAttr, newAttr) =>
         val nodeOldMap = oldAttr._2
         val nodeNewMap = newAttr._2
         val nodeTypeId = newAttr._1
         var newMap = Map[VertexId, NodeInfo]()           //initialization 

         specificNodeIdLst.foreach{(specificNodeIdType: (VertexId, Int, Double)) =>
           if (nodeNewMap(specificNodeIdType._1).spDistance <= nodeOldMap(specificNodeIdType._1).spDistance)
           {
               val spDistance = nodeNewMap(specificNodeIdType._1).spDistance
               val spNumber = nodeNewMap(specificNodeIdType._1).spNumber
               val newhierLevelDifference =  0    //nodeNewMap(specificNodeIdType._1).hierLevelDifference*(-1)          //-1*hierLevelDifference； downward inheritance
               val newClosenessScore = starQuery.calculateClosenessScore(spDistance, spNumber, newhierLevelDifference)
               val newLowerBoundCScore =  0 //math.min(starQuery.N*scala.math.pow(starQuery.ALPHA, (spDistance-newhierLevelDifference)), nodeNewMap(specificNodeIdType._1).lowerBoundCloseScore)

               val newUpperBoundCScore = 0   //calculateUpperBound(newLowerBoundCScore, spDistance, newhierLevelDifference)
               val tmpNodeInfo = nodeNewMap(specificNodeIdType._1).copy(closenessNodeScore = newClosenessScore,
                                                                  lowerBoundCloseScore = newLowerBoundCScore, upperBoundCloseScore = newUpperBoundCScore)  //update closenessNodeScore 
               newMap += (specificNodeIdType._1 -> tmpNodeInfo)
           }
           else
           {
               newMap += (specificNodeIdType._1 -> nodeOldMap(specificNodeIdType._1))
           }

        }

       (nodeTypeId, newMap)

      }.cache()

      val allNodesVisitedAnyOne =  g.vertices.filter{ case x=>
         val nodeMap = x._2._2

         //judge the nodes is visited from any one of the specific nodes
         def getAllVisiteFlag(nodeMap: Map[VertexId, NodeInfo]) ={             //define function
           var visitedFlag = false
           for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
               if (nodeInfo.spDistance != Long.MaxValue)        //any one exist
                   visitedFlag = true
           }
           visitedFlag
         }

        /* def excludeFromSpecificNodes(inputVal: VertexId)  = {        //exclude specificNodeIdLst
            var flag = false
             if (!specificNodeIdLst.map(_._1).contains(inputVal))
             {
               flag = true 
             }
           flag
          }
        */
        //  (getAllVisiteFlag(nodeMap) && excludeFromSpecificNodes(x._1))    //
         getAllVisiteFlag(nodeMap)         //

       }

       val allNodesVisitedNumber = allNodesVisitedAnyOne.count()
       print ("nonStarQueryGraphbfsTraverseTwoQueryNodes allNodesVisitedNumber: " + allNodesVisitedNumber + "\n")  
       
       twoPreviousOldAllNodesVisitedNumber = oldAllNodesVisitedNumber        //update the previous previous visited nodes number from previous nodes


       //val anotherUnknownDestNodeIdHashMap = nextUnknownDestNodeIdLst.map{s => (s._1, (s._2, s._3))}

       val visitedDestinationRdd = allNodesVisitedAnyOne.filter{
         case x=> 

          def getVisitedDestFlag(inputVal: VertexId) = {
             var flag = false
             if (nextUnknownDestNodeIdLst.map(_._1).contains(inputVal))
             {
               flag = true 
             }
             flag
          }

          getVisitedDestFlag(x._1)

       }
 
             
               //another termination condition: if all nextUnknownDestNodeIdLst are all visited, then the recycle termination
       //get the visited nodes for all the specific nodes
       val allNodesVisitedAllSpecifics =  g.vertices.filter{ case x=>
         val nodeMap = x._2._2

        def getAllVisiteFlag(nodeMap: Map[VertexId, NodeInfo]) ={             //define function
           var visitedFlag = true
           for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
               if (nodeInfo.spDistance == Long.MaxValue)        //any one exist
                   visitedFlag = false
           }
           visitedFlag
         }

          def getVisitedDestFlag(inputVal: VertexId) = {
             var flag = false
             if (nextUnknownDestNodeIdLst.map(_._1).contains(inputVal))
             {
               flag = true 
             }
             flag
          }
         
          (getAllVisiteFlag(nodeMap) && getVisitedDestFlag(x._1))    //

       }
      
        allNodesVisitedAllSpecificsNumber = allNodesVisitedAllSpecifics.count()
        
        //get candidateTuple number and their matching score from candidate nodes number
        val candidateTupleRdd = visitedDestinationRdd.map{
          case x=>
            
            def getVisitedCandidatesNodesTuple(destNodeId: VertexId, nodeMap: Map[VertexId, NodeInfo]) = {
              val candidateTupleLstBuffer = new ListBuffer[(List[VertexId], Double)]()        //nodeId1, nodeId2, closeness score
              for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
                if (nodeInfo.closenessNodeScore != 0)
                  {
                    candidateTupleLstBuffer += ((List(specNodeId, destNodeId), nodeInfo.closenessNodeScore))
                  }
              }
              candidateTupleLstBuffer              
            }
            
            getVisitedCandidatesNodesTuple(x._1, x._2._2)       //RDD's each element is a list
        }.flatMap(x => x)                                      //flatten the list
          
       
        //get the candidate pairs final matching score
        val currentNonStarResultRdd = candidateTupleRdd.map{
          
          x =>
          
          def getmatchingScoreTuple(nodeIdPair: List[VertexId]) = {
             var i = 0
            // val candidateNodeRDD = topKStarRstLstBuffer(i).map{x => (x._1, x._2._1)}         // => (nodeId, node matchingScore)
            var starQueryPrevScoreSum = 0.0
            while (i < nodeIdPair.size)
            {
              val nodeId = nodeIdPair(i)
              starQueryPrevScoreSum += starQueryNodeHashMap((i, nodeId)) //candidateNodeRDD.filter(x => x._1 == nodeId).take(1).head._2
              i = i + 1
            }
            starQueryPrevScoreSum += x._2       
            starQueryPrevScoreSum/(nodeIdPair.size + 1)
          }
          
          (x._1, getmatchingScoreTuple(x._1))
        }          // .takeOrdered(nonStarQuery_TOPK)(Ordering[Double].reverse.on(x=>x._2))
        
        
        print ("nonStarQueryGraphbfsTraverseTwoQueryNodes: " + currentNonStarResultRdd.count() + "\n")
       
        if (currentNonStarResultRdd.count() < nonStarQuery_TOPK)
        {
            topKNonStarResultRdd = currentNonStarResultRdd

        }
        else
        {
          val topKNonStarResultArray = currentNonStarResultRdd.takeOrdered(nonStarQuery_TOPK)(Ordering[Double].reverse.on(x=>x._2))
          print ("nonStarQueryGraphbfsTraverseTwoQueryNodes topKNonStarResultArray: " + nonStarQuery_TOPK + " " + topKNonStarResultArray.size + "\n")

          topKNonStarResultRdd = sc.parallelize(topKNonStarResultArray) 
           
        }
        
       topKNonStarResultRdd.take(5).foreach(println)
        
       print ("nonStarQueryGraphbfsTraverseTwoQueryNodes visitedDestinationRdd count: " + nonStarQuery_TOPK + " " +visitedDestinationRdd.count() + " " + currentNonStarResultRdd.count() + " " + topKNonStarResultRdd.count() + "\n")  

      // visitedDestinationRdd.take(5).foreach(println)
       oldAllNodesVisitedNumber = allNodesVisitedNumber           //update previous as the current

      }
      
     
      i = i + 2
             
    }
    
    val endTime = System.currentTimeMillis()   
    println("nonStarQueryGraphbfsTraverseTwoQueryNodes runtime: "+ (endTime-startTime) + " ms") 
    
    
    topKNonStarResultRdd
    
  }
  */
  
  /*
  //non Star query with two or more unknown nodes
  //input: previous star query results
  //output: final unknown query tuples and the final scores
  def nonStarQueryGraphbfsTraverseAnyQueryNodes[VD, ED](sc: SparkContext, graph: Graph[VD, ED], topKStarRstLstBuffer: ListBuffer[RDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))]], dstTypeIdLstBuffer: ListBuffer[Int]) = {
    
    val startTime = System.currentTimeMillis()              //System.nanoTime()

    val starQueryNodeHashMap = getStarQueryCandidateScoreHashMap(topKStarRstLstBuffer)   //get hashmap for previous star query id-> score
    print ("nonStarQueryGraphbfsTraverseAnyQueryNodes starQueryNodeHashMap: ", starQueryNodeHashMap)
    var i = 0
    
    var topKNonStarResultRdd: RDD[(List[VertexId], Double)] = sc.emptyRDD[(List[VertexId], Double)]          //Return Result RDD, (nodeId, matchingScore, lowerBound, upperBound, nodeMap)

    //record each iteration of non star query  unknown nodes traversal result
    var previousNonStarQueryRdd: RDD[(VertexId, (List[VertexId], Double))] = sc.emptyRDD[(VertexId, (List[VertexId], Double))]
    
    while (i < topKStarRstLstBuffer.length-1)      //  
    {
       //initialize the source list, as the new specificNodeIdLst
        val specificNodeIdLst = topKStarRstLstBuffer(i).map{x => (x._1, x._2._4, x._2._1)}         // => (nodeId, nodeTypeId, node matchingScore)
         .collect().toList.sortBy(x=>x._3)            // sorted by node matching score

        //val specificNodeIdLstHashMap = specificNodeIdLst.map{s => (s._1, (s._2, s._3))}


        //check the visited nodes have the another unkown  destination candidates nodes; that is in the topKStarRstLst(i+1) 
        val nextUnknownDestNodeIdLst = topKStarRstLstBuffer(i+1).map{x => (x._1, x._2._4, x._2._1)}         // => (nodeId, nodeTypeId, node matchingScore)
           .collect().toList.sortBy(x=>x._3)            // sorted by node matching score
        
        print ("nonStarQueryGraphbfsTraverseTwoQueryNodes specificNodeIdLsts: " + specificNodeIdLst +  "nextUnknownDestNodeIdLst "+ nextUnknownDestNodeIdLst + " " + i + "\n")  
       //initialize from first topKStarQuery as specificNodeList
       var g: Graph[(VD, Map[VertexId, NodeInfo]), ED] =
       graph.mapVertices((id, nodeIdType) => (nodeIdType, 
                                              specificNodeIdLst.map(specificNodeIdType=> specificNodeIdType._1-> NodeInfo(specificNodeIdType._1, 
                                                                                                                          specificNodeIdType._2, if (id == specificNodeIdType._1) 0 else Long.MaxValue, if (id == specificNodeIdType._1) 1 else 0, 0, 0.0, 0, if (id == specificNodeIdType._1) GREY.id else WHITE.id, if (id == specificNodeIdType._1) 1.0 else 0.0,  if (id == specificNodeIdType._1) 1 else {starQuery.N*scala.math.pow(starQuery.ALPHA, 1)})).toMap
       )).cache()

      // val dstTypeId = dstTypeIdLstBuffer(i+1)              //updated the dstTypeId

       //var topKResultRdd: RDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))] = sc.emptyRDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))]           //Return Result RDD, (nodeId, matchingScore, lowerBound, upperBound, nodeMap)
       

      var allNodesVisitedNumber: Long = 0L                         // all nodes visited
      var oldAllNodesVisitedNumber: Long = -1L                     //previous iteration nodes visited
      var twoPreviousOldAllNodesVisitedNumber: Long = -2L          //previous and previous iteration nodes visited
    
      var allNodesVisitedAllSpecificsNumber = 0L
       //aggregate the message 
       while (nextUnknownDestNodeIdLst.size != allNodesVisitedAllSpecificsNumber && twoPreviousOldAllNodesVisitedNumber != oldAllNodesVisitedNumber && allNodesVisitedNumber < graph.ops.numVertices)
       {
         val msgs: VertexRDD[(VD, Map[VertexId, NodeInfo])] = g.aggregateMessages[(VD, Map[VertexId, NodeInfo])](
         triplet => {
           val srcNodeMap = triplet.srcAttr._2
           //println("266 starQueryGraphbfsTraverse srcNodeMap: ", srcNodeMap)
           var dstNodeMap = triplet.dstAttr._2
           var newdstNodeMap = dstNodeMap

           specificNodeIdLst.foreach((specificNodeIdType: (VertexId, Int, Double)) => 
               if (srcNodeMap(specificNodeIdType._1).spDistance != Long.MaxValue && srcNodeMap(specificNodeIdType._1).spDistance + 1  < dstNodeMap(specificNodeIdType._1).spDistance)
               {
                   val specificNodeId = specificNodeIdType._1
                   //val specNodeIdType = specificNodeIdType._2  

                  //update spDist and parentId only
                   val tmpNodeInfo = srcNodeMap(specificNodeId).copy(spDistance = srcNodeMap(specificNodeId).spDistance+1, parentId = triplet.srcId)  
                   //update dstNodeMap 
                   newdstNodeMap += (specificNodeId -> tmpNodeInfo)              

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
            specificNodeIdLst.foreach((specificNodeIdType: (VertexId, Int, Double)) =>
               //keep current specificNodeId's map value
             if (nodeMapA(specificNodeIdType._1).spDistance < nodeMapB(specificNodeIdType._1).spDistance){  
               //update visit color,  lowerBoundCloseness Score
               val updatedLowerBoundCloseScore = 0      //calculateLowerBound(specificNodeIdType._1, nodeMapA)

               val tmpNodeInfo = nodeMapA(specificNodeIdType._1).copy(visitedColor = GREY.id, lowerBoundCloseScore = updatedLowerBoundCloseScore)  //update color visited
               newMap += (specificNodeIdType._1 -> tmpNodeInfo)       //update key -> value
               //key -> value
             }
             else if (nodeMapA(specificNodeIdType._1).spDistance == nodeMapB(specificNodeIdType._1).spDistance){   
               //update bound
               val updatedLowerBoundCloseScoreA = 0  // calculateLowerBound(specificNodeIdType._1, nodeMapA)
               val updatedLowerBoundCloseScoreB = 0      //calculateLowerBound(specificNodeIdType._1, nodeMapB)

               val updatedLowerBoundCloseScore =  updatedLowerBoundCloseScoreA + updatedLowerBoundCloseScoreB
               val tmpNodeInfo = nodeMapA(specificNodeIdType._1).copy(spNumber = nodeMapA(specificNodeIdType._1).spNumber+1, visitedColor = GREY.id, 
                                                                      lowerBoundCloseScore = updatedLowerBoundCloseScore)  //update spNumber
               newMap += (specificNodeIdType._1 -> tmpNodeInfo)

           }
           else
             {
               //nodeMapB(specificNodeIdType._1).lowerBoundCloseScore + ....
               val updatedLowerBoundCloseScore = 0  // calculateLowerBound(specificNodeIdType._1, nodeMapB)

               val tmpNodeInfo = nodeMapB(specificNodeIdType._1).copy(visitedColor = GREY.id, lowerBoundCloseScore = updatedLowerBoundCloseScore)  //update color visited
               newMap += (specificNodeIdType._1 -> tmpNodeInfo)
             }
           )

          (nodeTypeId, newMap)

         }                
       ).cache()

       //no updated messages
      //if (msgs.count == 0)
       //(topKResultRdd, pathAnswerRdd)
       // topKResultRdd                         //only return topKResultRdd

      g = g.ops.joinVertices(msgs) {
       (nodeId, oldAttr, newAttr) =>
         val nodeOldMap = oldAttr._2
         val nodeNewMap = newAttr._2
         val nodeTypeId = newAttr._1
         var newMap = Map[VertexId, NodeInfo]()           //initialization 

         specificNodeIdLst.foreach{(specificNodeIdType: (VertexId, Int, Double)) =>
           if (nodeNewMap(specificNodeIdType._1).spDistance <= nodeOldMap(specificNodeIdType._1).spDistance)
           {
               val spDistance = nodeNewMap(specificNodeIdType._1).spDistance
               val spNumber = nodeNewMap(specificNodeIdType._1).spNumber
               val newhierLevelDifference =  0    //nodeNewMap(specificNodeIdType._1).hierLevelDifference*(-1)          //-1*hierLevelDifference； downward inheritance
               val newClosenessScore = starQuery.calculateClosenessScore(spDistance, spNumber, newhierLevelDifference)
               val newLowerBoundCScore =  0 //math.min(starQuery.N*scala.math.pow(starQuery.ALPHA, (spDistance-newhierLevelDifference)), nodeNewMap(specificNodeIdType._1).lowerBoundCloseScore)

               val newUpperBoundCScore = 0   //calculateUpperBound(newLowerBoundCScore, spDistance, newhierLevelDifference)
               val tmpNodeInfo = nodeNewMap(specificNodeIdType._1).copy(closenessNodeScore = newClosenessScore,
                                                                  lowerBoundCloseScore = newLowerBoundCScore, upperBoundCloseScore = newUpperBoundCScore)  //update closenessNodeScore 
               newMap += (specificNodeIdType._1 -> tmpNodeInfo)
           }
           else
           {
               newMap += (specificNodeIdType._1 -> nodeOldMap(specificNodeIdType._1))
           }

        }

       (nodeTypeId, newMap)

      }.cache()

      val allNodesVisitedAnyOne =  g.vertices.filter{ case x=>
         val nodeMap = x._2._2

         //judge the nodes is visited from any one of the specific nodes
         def getAllVisiteFlag(nodeMap: Map[VertexId, NodeInfo]) ={             //define function
           var visitedFlag = false
           for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
               if (nodeInfo.spDistance != Long.MaxValue)        //any one exist
                   visitedFlag = true
           }
           visitedFlag
         }

        /* def excludeFromSpecificNodes(inputVal: VertexId)  = {        //exclude specificNodeIdLst
            var flag = false
             if (!specificNodeIdLst.map(_._1).contains(inputVal))
             {
               flag = true 
             }
           flag
          }
        */
        //  (getAllVisiteFlag(nodeMap) && excludeFromSpecificNodes(x._1))    //
         getAllVisiteFlag(nodeMap)         //

       }

       val allNodesVisitedNumber = allNodesVisitedAnyOne.count()
       print ("nonStarQueryGraphbfsTraverseTwoQueryNodes allNodesVisitedNumber: " + allNodesVisitedNumber + "\n")  
       
       twoPreviousOldAllNodesVisitedNumber = oldAllNodesVisitedNumber        //update the previous previous visited nodes number from previous nodes


       //val anotherUnknownDestNodeIdHashMap = nextUnknownDestNodeIdLst.map{s => (s._1, (s._2, s._3))}

       val visitedDestinationRdd = allNodesVisitedAnyOne.filter{
         case x=> 

          def getVisitedDestFlag(inputVal: VertexId) = {
             var flag = false
             if (nextUnknownDestNodeIdLst.map(_._1).contains(inputVal))
             {
               flag = true 
             }
             flag
          }

          getVisitedDestFlag(x._1)

       }
             
        //another termination condition: if all nextUnknownDestNodeIdLst are all visited, then the recycle termination
       //get the visited nodes for all the specific nodes
       val allNodesVisitedAllSpecifics =  g.vertices.filter{ case x=>
         val nodeMap = x._2._2

        def getAllVisiteFlag(nodeMap: Map[VertexId, NodeInfo]) ={             //define function
           var visitedFlag = true
           for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
               if (nodeInfo.spDistance == Long.MaxValue)          //any one exist
                   visitedFlag = false
           }
           visitedFlag
         }

          def getVisitedDestFlag(inputVal: VertexId) = {
             var flag = false
             if (nextUnknownDestNodeIdLst.map(_._1).contains(inputVal))
             {
               flag = true 
             }
             flag
          }
         
          (getAllVisiteFlag(nodeMap) && getVisitedDestFlag(x._1))    //

       }
      
        allNodesVisitedAllSpecificsNumber = allNodesVisitedAllSpecifics.count()
        
      
    
      //  print ("nonStarQueryGraphbfsTraverseTwoQueryNodes visitedDestinationRdd count: " + nonStarQuery_TOPK + " " +visitedDestinationRdd.count() + " " + currentNonStarResultRdd.count() + " " + topKNonStarResultRdd.count() + "\n")  

        // visitedDestinationRdd.take(5).foreach(println)
        oldAllNodesVisitedNumber = allNodesVisitedNumber           //update previous as the current

      }
      
      
      val visitedDestinationEndRdd = g.vertices.filter{
         case x=> 

          def getVisitedDestFlag(inputVal: VertexId) = {
             var flag = false
             if (nextUnknownDestNodeIdLst.map(_._1).contains(inputVal))
             {
               flag = true 
             }
             flag
          }

          getVisitedDestFlag(x._1)

       }
       
      //get candidateTuple number and their matching score from candidate nodes number
      val candidateTupleRdd = visitedDestinationEndRdd.map{
        case x=>

          def getVisitedCandidatesNodesTuple(destNodeId: VertexId, nodeMap: Map[VertexId, NodeInfo]) = {
            val candidateTupleLstBuffer = new ListBuffer[((VertexId, VertexId), Double)]()        //nodeId1, nodeId2, closeness score
            for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
              if (nodeInfo.closenessNodeScore != 0)
                {
                  candidateTupleLstBuffer += (((specNodeId, destNodeId), nodeInfo.closenessNodeScore))
                }
            }
            candidateTupleLstBuffer              
          }

          getVisitedCandidatesNodesTuple(x._1, x._2._2)       //RDD's each element is a list
      }.flatMap(x => x)                                      //flatten the list


      //println("nonStarQueryGraphbfsTraverseTwoQueryNodes ttttttttttttt i: " + i +"\n") 

      //get the candidate pairs final matching score
      val count = i          //necessary, in case of parallelling, i has updated outside, but we dont need to the value i outside
      val currentNonStarResultRdd = candidateTupleRdd.map{x =>
         val specNodeId = x._1._1

         def getmatchingScoreTuple(specinodeId: VertexId) = {
            var starQueryPrevScoreSum = 0.0
            //println("nonStarQueryGraphbfsTraverseTwoQueryNodes ssssssssssssss i: " + i + " " + count +" " + specinodeId + "\n") 
            starQueryPrevScoreSum += starQueryNodeHashMap((count, specinodeId)) //candidateNodeRDD.filter(x => x._1 == nodeId).take(1).head._2
            starQueryPrevScoreSum += x._2       
            
            starQueryPrevScoreSum            // only 2 here
        }

        (x._1._1, (x._1._2, getmatchingScoreTuple(x._1._1)))       //（specnodeId, (destNodeId, score))

      }       // .takeOrdered(nonStarQuery_TOPK)(Ordering[Double].reverse.on(x=>x._2))

      //get the current top-k result

      if (0 == i)
      {
          previousNonStarQueryRdd = currentNonStarResultRdd.map{ x => 
                (x._2._1, (List(x._1), x._2._2))
            }

          print ("nonStarQueryGraphbfsTraverseTwoQueryNodes previousNonStarQueryRddwwwwwwwwwwwwww: " + i + " " +previousNonStarQueryRdd.count() + " \n")
          previousNonStarQueryRdd.take(10).foreach(println)
      }
      else        // if(topKStarRstLstBuffer.length-1 != i)
      {

            print ("nonStarQueryGraphbfsTraverseTwoQueryNodes currentNonStarResultRddxxxxxxxxxxx: " + i + " \n" )
            currentNonStarResultRdd.take(10).foreach(println)
            print ("nonStarQueryGraphbfsTraverseTwoQueryNodes previousNonStarQueryRddxxxxxxxxxxx: " + i + " \n")
            previousNonStarQueryRdd.take(10).foreach(println)
            print ("nonStarQueryGraphbfsTraverseTwoQueryNodes previousNonStarQueryRddxxxxxxxxxxxyyyy: " + i + " " +previousNonStarQueryRdd.count() + " \n")
           //join the current and previous result 
           val unionCurrentRdd = currentNonStarResultRdd.join(previousNonStarQueryRdd)

           previousNonStarQueryRdd = unionCurrentRdd.map{      //(nodeId, (nodeId, score)) join (nodeId, (List(nodeId), score))
             x =>                  //(specNodeId, (nextNodeId, currentScore), (List(nodeId), prevscore))) 
             val nextNodeId = x._2._1._1
             val newScore = (x._2._1._2 + x._2._2._2)               // (x._2._1._2 + x._2._2._2) / 2
             val newNodeIdLst = x._1::x._2._2._1
             (nextNodeId, (newNodeIdLst, newScore))
           }

        print ("nonStarQueryGraphbfsTraverseTwoQueryNodes unionCurrentRdd previousNonStarQueryRdd:" + " " +unionCurrentRdd.count() 
                + " " + previousNonStarQueryRdd.count() + " " + currentNonStarResultRdd.count() + " \n")

      }

      print ("nonStarQueryGraphbfsTraverseTwoQueryNodes currentNonStarResultRdd: " + currentNonStarResultRdd.count() + "\n")
      //keep the current star query list visited result
      i = i + 1               //？ i = i + 1
             
    }
    
    
    if (previousNonStarQueryRdd.count() < nonStarQuery_TOPK)
    {
        topKNonStarResultRdd = previousNonStarQueryRdd.map{ x =>
          (x._1::x._2._1, (x._2._2 + starQueryNodeHashMap((i, x._1)))/(2*topKStarRstLstBuffer.length-1))
        }

    }
    else
    {
      val topKNonStarResultArray = previousNonStarQueryRdd.map{ x =>
          (x._1::x._2._1, (x._2._2 + starQueryNodeHashMap((i, x._1)))/(2*topKStarRstLstBuffer.length-1))

        }.takeOrdered(nonStarQuery_TOPK)(Ordering[Double].reverse.on(x=>x._2))
      print ("nonStarQueryGraphbfsTraverseTwoQueryNodes topKNonStarResultArray: " + nonStarQuery_TOPK + " " + topKNonStarResultArray.size + "\n")

      topKNonStarResultRdd = sc.parallelize(topKNonStarResultArray) 

    }

    topKNonStarResultRdd.take(5).foreach(println)
       
    //println("nonStarQueryGraphbfsTraverseTwoQueryNodes ppppppppppreviousNonStarQueryRdd result i: ", previousNonStarQueryRdd.count()) 
    //previousNonStarQueryRdd. take(5).foreach(println)
    
    val endTime = System.currentTimeMillis()   
    println("nonStarQueryGraphbfsTraverseTwoQueryNodes runtime: "+ (endTime-startTime) + " ms") 
    
    
    topKNonStarResultRdd
    
  }
*/

  
   //non Star query with two or more unknown nodes
  //input: previous star query results
  //output: final unknown query tuples and the final scores
  def nonStarQueryGraphbfsTraverseAnyQueryNodesWithPruningBounds[VD, ED](sc: SparkContext, graph: Graph[VD, ED], topKStarRstLstBuffer: ListBuffer[RDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))]], dstTypeIdLstBuffer: ListBuffer[Int]) = {
    
    val startTime = System.currentTimeMillis()              //System.nanoTime()

    val starQueryNodeHashMap = getStarQueryCandidateScoreHashMap(topKStarRstLstBuffer)   //get hashmap for previous star query id-> score
    //print ("nonStarQueryGraphbfsTraverseAnyQueryNodes starQueryNodeHashMap: ", starQueryNodeHashMap)
    var i = 0
    
    var topKNonStarResultRdd: RDD[(List[VertexId], Double)] = sc.emptyRDD[(List[VertexId], Double)]          //Return Result RDD, (nodeId, matchingScore, lowerBound, upperBound, nodeMap)

    //record each iteration of non star query  unknown nodes traversal result
    var previousNonStarQueryRdd: RDD[(VertexId, (List[VertexId], Double))] = sc.emptyRDD[(VertexId, (List[VertexId], Double))]
    
    while (i < topKStarRstLstBuffer.length-1)      //  
    {
      
      
      var topKKthLowerBoundScore = 0.0        //initialize for each candidate embedding enumeration traverse bounds
       //initialize the source list, as the new specificNodeIdLst
        val specificNodeIdLst = topKStarRstLstBuffer(i).map{x => (x._1, x._2._4, x._2._1)}         // => (nodeId, nodeTypeId, node matchingScore)
         .collect().toList.sortBy(x=>x._3)            // sorted by node matching score

        //val specificNodeIdLstHashMap = specificNodeIdLst.map{s => (s._1, (s._2, s._3))}


        //check the visited nodes have the another unkown  destination candidates nodes; that is in the topKStarRstLst(i+1) 
        val nextUnknownDestNodeIdLst = topKStarRstLstBuffer(i+1).map{x => (x._1, x._2._4, x._2._1)}         // => (nodeId, nodeTypeId, node matchingScore)
           .collect().toList.sortBy(x=>x._3)            // sorted by node matching score
        
        //print ("nonStarQueryGraphbfsTraverseTwoQueryNodes specificNodeIdLsts: " + specificNodeIdLst +  "nextUnknownDestNodeIdLst "+ nextUnknownDestNodeIdLst + " " + i + "\n")  
       //initialize from first topKStarQuery as specificNodeList
       var g: Graph[(VD, Map[VertexId, NodeInfo]), ED] =
       graph.mapVertices((id, nodeIdType) => (nodeIdType, 
                                              specificNodeIdLst.map(specificNodeIdType=> specificNodeIdType._1-> NodeInfo(specificNodeIdType._1, 
                                                                                                                          specificNodeIdType._2, if (id == specificNodeIdType._1) 0 else Long.MaxValue, if (id == specificNodeIdType._1) 1 else 0, 0, 0.0, 0, if (id == specificNodeIdType._1) GREY.id else WHITE.id, if (id == specificNodeIdType._1) 1.0 else 0.0,  if (id == specificNodeIdType._1) 1 else {starQuery.N*scala.math.pow(starQuery.ALPHA, 1)})).toMap
       )).cache()

      // val dstTypeId = dstTypeIdLstBuffer(i+1)              //updated the dstTypeId

       //var topKResultRdd: RDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))] = sc.emptyRDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))]           //Return Result RDD, (nodeId, matchingScore, lowerBound, upperBound, nodeMap)
       

      var allNodesVisitedNumber: Long = 0L                         // all nodes visited
      var oldAllNodesVisitedNumber: Long = -1L                     //previous iteration nodes visited
      var twoPreviousOldAllNodesVisitedNumber: Long = -2L          //previous and previous iteration nodes visited
    
      var allNodesVisitedAllSpecificsNumber = 0L
       //aggregate the message 
       while (nextUnknownDestNodeIdLst.size != allNodesVisitedAllSpecificsNumber && twoPreviousOldAllNodesVisitedNumber != oldAllNodesVisitedNumber && allNodesVisitedNumber < graph.ops.numVertices)
       {
         val msgs: VertexRDD[(VD, Map[VertexId, NodeInfo])] = g.aggregateMessages[(VD, Map[VertexId, NodeInfo])](
         triplet => {
           val srcNodeMap = triplet.srcAttr._2
           //println("266 starQueryGraphbfsTraverse srcNodeMap: ", srcNodeMap)
           var dstNodeMap = triplet.dstAttr._2
           var newdstNodeMap = dstNodeMap

           specificNodeIdLst.foreach((specificNodeIdType: (VertexId, Int, Double)) => 
               if (srcNodeMap(specificNodeIdType._1).visitedColor != RED.id && srcNodeMap(specificNodeIdType._1).spDistance != Long.MaxValue && srcNodeMap(specificNodeIdType._1).spDistance + 1  < dstNodeMap(specificNodeIdType._1).spDistance)
               {
                   val specificNodeId = specificNodeIdType._1
                   //val specNodeIdType = specificNodeIdType._2  

                  //update spDist and parentId only
                   val tmpNodeInfo = srcNodeMap(specificNodeId).copy(spDistance = srcNodeMap(specificNodeId).spDistance+1, parentId = triplet.srcId)  
                   //update dstNodeMap 
                   newdstNodeMap += (specificNodeId -> tmpNodeInfo)              

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
            specificNodeIdLst.foreach((specificNodeIdType: (VertexId, Int, Double)) =>
              
               //keep current specificNodeId's map value
             if (nodeMapA(specificNodeIdType._1).spDistance < nodeMapB(specificNodeIdType._1).spDistance){  
               //update visit color,  lowerBoundCloseness Score
               val updatedLowerBoundCloseScore = 0 // starQuery.calculateLowerBound(specificNodeIdType._1, nodeMapA)

               val tmpNodeInfo = nodeMapA(specificNodeIdType._1).copy(visitedColor = GREY.id, lowerBoundCloseScore = updatedLowerBoundCloseScore)  //update color visited
               newMap += (specificNodeIdType._1 -> tmpNodeInfo)       //update key -> value
               //key -> value
             }
             else if (nodeMapA(specificNodeIdType._1).spDistance == nodeMapB(specificNodeIdType._1).spDistance){   
               //update bound
               val updatedLowerBoundCloseScoreA = 0  // starQuery.calculateLowerBound(specificNodeIdType._1, nodeMapA)
               val updatedLowerBoundCloseScoreB = 0 //  starQuery.calculateLowerBound(specificNodeIdType._1, nodeMapB)

               val updatedLowerBoundCloseScore =  updatedLowerBoundCloseScoreA + updatedLowerBoundCloseScoreB
               val tmpNodeInfo = nodeMapA(specificNodeIdType._1).copy(spNumber = nodeMapA(specificNodeIdType._1).spNumber+1, visitedColor = GREY.id, 
                                                                      lowerBoundCloseScore = updatedLowerBoundCloseScore)  //update spNumber
               newMap += (specificNodeIdType._1 -> tmpNodeInfo)

           }
           else
             {
               //nodeMapB(specificNodeIdType._1).lowerBoundCloseScore + ....
               val updatedLowerBoundCloseScore = 0 // starQuery.calculateLowerBound(specificNodeIdType._1, nodeMapB)

               val tmpNodeInfo = nodeMapB(specificNodeIdType._1).copy(visitedColor = GREY.id, lowerBoundCloseScore = updatedLowerBoundCloseScore)  //update color visited
               newMap += (specificNodeIdType._1 -> tmpNodeInfo)
             }
             
           )

          (nodeTypeId, newMap)

         }                
       ).cache()

       //no updated messages
      //if (msgs.count == 0)
       //(topKResultRdd, pathAnswerRdd)
       // topKResultRdd                         //only return topKResultRdd

        g = g.ops.joinVertices(msgs) {
         (nodeId, oldAttr, newAttr) =>
           val nodeOldMap = oldAttr._2
           val nodeNewMap = newAttr._2
           val nodeTypeId = newAttr._1
           var newMap = Map[VertexId, NodeInfo]()           //initialization 

           specificNodeIdLst.foreach{(specificNodeIdType: (VertexId, Int, Double)) =>
             if (nodeNewMap(specificNodeIdType._1).spDistance <= nodeOldMap(specificNodeIdType._1).spDistance)
             {
                 val spDistance = nodeNewMap(specificNodeIdType._1).spDistance
                 val spNumber = nodeNewMap(specificNodeIdType._1).spNumber
                 val newhierLevelDifference =  0       //nodeNewMap(specificNodeIdType._1).hierLevelDifference*(-1)          //-1*hierLevelDifference； downward inheritance
                 val newClosenessScore = starQuery.calculateClosenessScore(spDistance, spNumber, newhierLevelDifference)
                 val newLowerBoundCScore =  math.min(starQuery.N*scala.math.pow(starQuery.ALPHA, (spDistance-newhierLevelDifference)), nodeNewMap(specificNodeIdType._1).lowerBoundCloseScore)

                 val newUpperBoundCScore = starQuery.calculateUpperBound(newLowerBoundCScore, spDistance, newhierLevelDifference)
                 val tmpNodeInfo = nodeNewMap(specificNodeIdType._1).copy(closenessNodeScore = newClosenessScore,
                                                                    lowerBoundCloseScore = newLowerBoundCScore, upperBoundCloseScore = newUpperBoundCScore)      //update closenessNodeScore 
                 newMap += (specificNodeIdType._1 -> tmpNodeInfo)
             }
             else
             {
                 newMap += (specificNodeIdType._1 -> nodeOldMap(specificNodeIdType._1))
             }

          }

         (nodeTypeId, newMap)

        }.cache()

   
        val allNodesVisitedAnyOne = g.vertices.filter{ case x=>
           val nodeMap = x._2._2

           //judge the nodes is visited from any one of the specific nodes
           def getAllVisiteFlag(nodeMap: Map[VertexId, NodeInfo]) ={             //define function
             var visitedFlag = false
             for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
                 if (nodeInfo.spDistance != Long.MaxValue)        //any one exist
                     visitedFlag = true
             }
             visitedFlag
           }

          /* def excludeFromSpecificNodes(inputVal: VertexId)  = {        //exclude specificNodeIdLst
              var flag = false
               if (!specificNodeIdLst.map(_._1).contains(inputVal))
               {
                 flag = true 
               }
             flag
            }
          */
          //  (getAllVisiteFlag(nodeMap) && excludeFromSpecificNodes(x._1))    //
           getAllVisiteFlag(nodeMap)         //

         }

        //update bounds of each node in graph g  
        g = starQuery.setnodeIdColorForBound(allNodesVisitedAnyOne, g)                 //update bounding nodes color


         val allNodesVisitedNumber = allNodesVisitedAnyOne.count()
         //print ("nonStarQueryGraphbfsTraverseTwoQueryNodes allNodesVisitedNumber: " + allNodesVisitedNumber + "\n")  

         twoPreviousOldAllNodesVisitedNumber = oldAllNodesVisitedNumber        //update the previous previous visited nodes number from previous nodes


         //val anotherUnknownDestNodeIdHashMap = nextUnknownDestNodeIdLst.map{s => (s._1, (s._2, s._3))}

         val visitedDestinationRdd = allNodesVisitedAnyOne.filter{
           case x=> 

            def getVisitedDestFlag(inputVal: VertexId) = {
               var flag = false
               if (nextUnknownDestNodeIdLst.map(_._1).contains(inputVal))
               {
                 flag = true 
               }
               flag
            }

            getVisitedDestFlag(x._1)

         }

         //another termination condition: if all nextUnknownDestNodeIdLst are all visited, then the recycle termination
         //get the visited nodes for all the specific nodes
         val allNodesVisitedAllSpecifics =  g.vertices.filter{ case x=>
           val nodeMap = x._2._2

          def getAllVisiteFlag(nodeMap: Map[VertexId, NodeInfo]) ={             //define function
             var visitedFlag = true
             for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
                 if (nodeInfo.spDistance == Long.MaxValue)          //any one exist
                     visitedFlag = false
             }
             visitedFlag
           }

            def getVisitedDestFlag(inputVal: VertexId) = {
               var flag = false
               if (nextUnknownDestNodeIdLst.map(_._1).contains(inputVal))
               {
                 flag = true 
               }
               flag
            }

            (getAllVisiteFlag(nodeMap) && getVisitedDestFlag(x._1))    //

         }

          allNodesVisitedAllSpecificsNumber = allNodesVisitedAllSpecifics.count()

          //print ("nonStarQueryGraphbfsTraverseTwoQueryNodes visitedDestinationRdd count: " + nonStarQuery_TOPK + " " +visitedDestinationRdd.count() + " " + currentNonStarResultRdd.count() + " " + topKNonStarResultRdd.count() + "\n")  

          // visitedDestinationRdd.take(5).foreach(println)
          oldAllNodesVisitedNumber = allNodesVisitedNumber           //update previous as the current

         //update topKKthLowerBoundScore;        how?
         val currentIterateNodeResult = visitedDestinationRdd.map{
            case x=>

            def getVisitedCandidatesNodesTuple(destNodeId: VertexId, nodeMap: Map[VertexId, NodeInfo]) = {
              val candidateTupleLstBuffer = new ListBuffer[((VertexId, VertexId), Map[VertexId, NodeInfo])]()        //nodeId1, nodeId2, closeness score
              for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
                if (nodeInfo.closenessNodeScore != 0)
                  {
                    candidateTupleLstBuffer += (((specNodeId, destNodeId), nodeMap))
                  }
              }
              candidateTupleLstBuffer              
            }

             getVisitedCandidatesNodesTuple(x._1, x._2._2)       //RDD's each element is a list
         }.flatMap(x => x)              

        if (currentIterateNodeResult.count > nonStarQuery_TOPK)
        {
          
          val topKResultRddArray =  currentIterateNodeResult.map(x=>
          starQuery.calculateMatchingScoreLowerBound(x._2)
          ).takeOrdered(nonStarQuery_TOPK)(Ordering[Double].on(x=>x))    //sort by matching score calculateNodeScoreStarquery result
       
          //get the kth smallest lower bound score in the topKResultRddArray
          topKKthLowerBoundScore = topKResultRddArray.head    //.sortBy(x=>x)
          
          //println("nonStarQueryGraphbfsTraverseTwoQueryNodes topKKthLowerBoundScore tttt i: " + topKResultRddArray.toList +" " + topKKthLowerBoundScore + "\n") 

        }  
        
      }
      
      
      val visitedDestinationEndRdd = g.vertices.filter{
         case x=> 

          def getVisitedDestFlag(inputVal: VertexId) = {
             var flag = false
             if (nextUnknownDestNodeIdLst.map(_._1).contains(inputVal))
             {
               flag = true 
             }
             flag
          }

          getVisitedDestFlag(x._1)

       }
       
      //get candidateTuple number and their matching score from candidate nodes number
      val candidateTupleRdd = visitedDestinationEndRdd.map{
        case x=>

          def getVisitedCandidatesNodesTuple(destNodeId: VertexId, nodeMap: Map[VertexId, NodeInfo]) = {
            val candidateTupleLstBuffer = new ListBuffer[((VertexId, VertexId), Double)]()        //nodeId1, nodeId2, closeness score
            for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
             // if (nodeInfo.closenessNodeScore != 0)
             //   {
                  candidateTupleLstBuffer += (((specNodeId, destNodeId), nodeInfo.closenessNodeScore))
             //   }
            }
           // println("nonStarQueryGraphbfsTraverseTwoQueryNodes candidateTupleLstBuffer ii: " + i + " " + candidateTupleLstBuffer.length + "\n") 
            candidateTupleLstBuffer 
          }

          getVisitedCandidatesNodesTuple(x._1, x._2._2)       //RDD's each element is a list
      }.flatMap(x => x)                                      //flatten the list


    //  println("nonStarQueryGraphbfsTraverseTwoQueryNodes ttttttttttttt i: " + i + " " + visitedDestinationEndRdd.count() + " " + candidateTupleRdd.count() + "\n") 

      //get the candidate pairs final matching score
      val count = i          //necessary, in case of parallelling, i has updated outside, but we dont need to the value i outside
      val currentNonStarResultRdd = candidateTupleRdd.map{x =>
         val specNodeId = x._1._1

         def getmatchingScoreTuple(specinodeId: VertexId) = {
            var starQueryPrevScoreSum = 0.0
            //println("nonStarQueryGraphbfsTraverseTwoQueryNodes ssssssssssssss i: " + i + " " + count +" " + specinodeId + "\n") 
            starQueryPrevScoreSum += starQueryNodeHashMap((count, specinodeId)) //candidateNodeRDD.filter(x => x._1 == nodeId).take(1).head._2
            starQueryPrevScoreSum += x._2       
            
            starQueryPrevScoreSum            // only 2 here
        }

        (x._1._1, (x._1._2, getmatchingScoreTuple(x._1._1)))       //（specnodeId, (destNodeId, score))

      }       // .takeOrdered(nonStarQuery_TOPK)(Ordering[Double].reverse.on(x=>x._2))

      //get the current top-k result

      if (0 == i)
      {
          previousNonStarQueryRdd = currentNonStarResultRdd.map{ x => 
                (x._2._1, (List(x._1), x._2._2))
            }
          //print ("nonStarQueryGraphbfsTraverseTwoQueryNodes previousNonStarQueryRddwwwwwwwwwwwwww: " + i + " " +previousNonStarQueryRdd.count() +  " " + candidateTupleRdd.count() + " \n")
          //previousNonStarQueryRdd.take(10).foreach(println)
      }
      else        // if(topKStarRstLstBuffer.length-1 != i)
      {

            //print ("nonStarQueryGraphbfsTraverseTwoQueryNodes currentNonStarResultRddxxxxxxxxxxx: " + i + " \n" )
            //currentNonStarResultRdd.take(10).foreach(println)
           // print ("nonStarQueryGraphbfsTraverseTwoQueryNodes previousNonStarQueryRddxxxxxxxxxxx: " + i + " \n")
           // previousNonStarQueryRdd.take(10).foreach(println)
           // print ("nonStarQueryGraphbfsTraverseTwoQueryNodes previousNonStarQueryRddxxxxxxxxxxxyyyy: " + i + " " +previousNonStarQueryRdd.count() + " \n")
           //join the current and previous result 
           val unionCurrentRdd = currentNonStarResultRdd.join(previousNonStarQueryRdd)

           previousNonStarQueryRdd = unionCurrentRdd.map{      //(nodeId, (nodeId, score)) join (nodeId, (List(nodeId), score))
             x =>                  //(specNodeId, (nextNodeId, currentScore), (List(nodeId), prevscore))) 
             val nextNodeId = x._2._1._1
             val newScore = (x._2._1._2 + x._2._2._2)               // (x._2._1._2 + x._2._2._2) / 2
             val newNodeIdLst = x._1::x._2._2._1
             (nextNodeId, (newNodeIdLst, newScore))
           }

        //print ("nonStarQueryGraphbfsTraverseTwoQueryNodes unionCurrentRdd previousNonStarQueryRdd currentNonStarResultRdd:" + " " +unionCurrentRdd.count() 
        //        + " " + previousNonStarQueryRdd.count() + " " + currentNonStarResultRdd.count() + " \n")

      }

      //print ("nonStarQueryGraphbfsTraverseTwoQueryNodes currentNonStarResultRdd: " + currentNonStarResultRdd.count() + "\n")
      //keep the current star query list visited result
      i = i + 1               //？ i = i + 1
             
    }
    
    if (previousNonStarQueryRdd.count() < nonStarQuery_TOPK)
    {
        topKNonStarResultRdd = previousNonStarQueryRdd.map{ x =>
          (x._1::x._2._1, (x._2._2 + starQueryNodeHashMap((i, x._1)))/(2*topKStarRstLstBuffer.length-1))
        }
    }
    else
    {
      val topKNonStarResultArray = previousNonStarQueryRdd.map{ x =>
          (x._1::x._2._1, (x._2._2 + starQueryNodeHashMap((i, x._1)))/(2*topKStarRstLstBuffer.length-1))

        }.takeOrdered(nonStarQuery_TOPK)(Ordering[Double].reverse.on(x=>x._2))
      //print ("nonStarQueryGraphbfsTraverseTwoQueryNodes topKNonStarResultArray: " + nonStarQuery_TOPK + " " + topKNonStarResultArray.size + "\n")

      topKNonStarResultRdd = sc.parallelize(topKNonStarResultArray) 

    }

    //topKNonStarResultRdd.take(5).foreach(println)
       
    //println("nonStarQueryGraphbfsTraverseTwoQueryNodes ppppppppppreviousNonStarQueryRdd result i: ", previousNonStarQueryRdd.count()) 
    //previousNonStarQueryRdd. take(5).foreach(println)
    
    val endTime = System.currentTimeMillis()   
    //println("nonStarQueryGraphbfsTraverseTwoQueryNodes runtime: "+ (endTime-startTime) + " ms") 
 
    topKNonStarResultRdd
    
  }

  
  //entry to execute non star query 
  def nonStarQueryExecute[VD, ED](sc: SparkContext, graph: Graph[VD, ED], specificNodeIdTwoDimensionLst: List[List[(VertexId,Int)]], dstTypeIdLstBuffer: ListBuffer[Int], nonStarQueryTOPK: Int, databaseType: Int, inputFileNodeInfoPath: String, outputFilePath: String, runTimeoutputFilePath: String, hierarchialRelation: Boolean) = {
      
    nonStarQuery_TOPK = nonStarQueryTOPK
    
    var topKStarRstLst = new ListBuffer[RDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))]]()
    var i = 0
    val startTime = System.currentTimeMillis()              //System.nanoTime()
    //print ("specificNodeIdTwoDimensionLst size: " + specificNodeIdTwoDimensionLst(0) + " " + i + " \n")
    while(i < specificNodeIdTwoDimensionLst.size)
    {
        val dstTypeId = dstTypeIdLstBuffer(i)
        val topKResultRdd = starQuery.starQueryGraphbfsTraverseWithBoundPruning(sc, graph, specificNodeIdTwoDimensionLst(i), dstTypeId, databaseType, null, hierarchialRelation)
       
        //when return paths
        //val answers = starQuery.starQueryGraphbfsTraverseWithBoundPruning(sc, graph, specificNodeIdTwoDimensionLst(i), dstTypeId, databaseType, null, hierarchialRelation)
       // val topKResultRdd = answers._1
        //val pathAnswerRdd = answers._2
        //topKResultRdd.take(5).foreach(println)
        //print ("i: " + specificNodeIdTwoDimensionLst(i) + " " + i + " \n")
        //pathAnswerRdd.take(5).foreach(println)

        topKStarRstLst  += topKResultRdd
        i = i + 1
    }
    
    //print ("size: " + topKStarRstLst.size +  " \n")
    
    // val topKNonStarResultRdd = nonStarQueryGraphbfsTraverseTwoQueryNodes(sc, graph, topKStarRstLst, dstTypeIdLst)
    val topKNonStarResultRdd = nonStarQueryGraphbfsTraverseAnyQueryNodesWithPruningBounds(sc, graph, topKStarRstLst, dstTypeIdLstBuffer)
    
    val endTime = System.currentTimeMillis()   
    //println("nonStarQueryExecute whole runtime: "+ (endTime-startTime) + " ms") 
    print ("topKNonStarResultRdd size: " +  topKNonStarResultRdd.count() + " \n")
    if (outputFilePath != null)
    {
        topKNonStarResultRdd.coalesce(1).saveAsTextFile(outputFilePath)        //coalesce into 1 file, it is small data output
    }
    
    
    if (runTimeoutputFilePath != null)
    {
        val runtTimefile = new File(runTimeoutputFilePath)
        val bw = new BufferedWriter(new FileWriter(runtTimefile))
        bw.write("Non-star Query TOP " + nonStarQuery_TOPK.toString + " " + "runtime: "+ (endTime-startTime).toString + " ms\n" )
        bw.close()
    }
    
  }
  
}
