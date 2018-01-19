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
  

  //get all visited nodes' matching lower and upper bound score
def nonStarQuerySetnodeIdColorForBound[VD, ED](allNodesVisited: VertexRDD[(VD, Map[VertexId, NodeInfo])], g: Graph[(VD, Map[VertexId, NodeInfo]), ED], starQueryNodeHashMap: Map[(Int, VertexId), Double], candidateIterNo: Int, topKKthLowerBoundScore: Double) = {
  
    val updatedNodeRdd = allNodesVisited.map{
      case (nodeId, (nodeIdType, nodeMap))=>
        
        var newMap = Map[VertexId, NodeInfo]()

        var matchingScoreUpperBound = starQuery.calculateMatchingScoreUpperBound(nodeMap) 
        for ((specNodeId, nodeInfo) <- nodeMap){
          matchingScoreUpperBound = matchingScoreUpperBound // + starQueryNodeHashMap((candidateIterNo, specNodeId))   //add the previous star query candidate score
          if (matchingScoreUpperBound != 0 && matchingScoreUpperBound <= topKKthLowerBoundScore)          //upperbound less than the kth lowerbound score
          {
             val tmpNodeInfo = nodeMap(specNodeId).copy(visitedColor = RED.id)  //update color visited
             newMap += (specNodeId -> tmpNodeInfo)
            // (nodeId, (nodeIdType, newMap))      
          }
         else
         {
           newMap += (specNodeId -> nodeMap(specNodeId))
          // (nodeId, (nodeIdType, nodeMap))       
         }
       }
       (nodeId, (nodeIdType, newMap))  
  }
    
  val newG = g.ops.joinVertices(updatedNodeRdd) {
        (nodeId, oldAttr, newAttr) => 
         
        newAttr
        
  }.cache()                      //cache or not
 
  // println("372 xxxxxxx node vertices edgecount: ", g.vertices.count, newG.vertices.count)
  newG
  
}

  
   //non Star query with two or more unknown nodes
  //input: previous star query results
  //output: final unknown query tuples and the final scores
  def nonStarQueryGraphbfsTraverseAnyQueryNodesWithPruningBounds[VD, ED](sc: SparkContext, graph: Graph[VD, ED], topKStarRstLstBuffer: ListBuffer[RDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))]], dstTypeIdLst: List[Int], databaseType: Int, hierarchialRelation: Boolean) = {
    
    val startTime = System.currentTimeMillis()              //System.nanoTime()

    val starQueryNodeHashMap = getStarQueryCandidateScoreHashMap(topKStarRstLstBuffer)   //get hashmap for previous star query id-> score
    //print ("nonStarQueryGraphbfsTraverseAnyQueryNodes starQueryNodeHashMap: ", starQueryNodeHashMap)
    var i = 0
    
    var topKNonStarResultRdd: RDD[(List[VertexId], Double)] = sc.emptyRDD[(List[VertexId], Double)]      //Return Result RDD, (nodeId, matchingScore, lowerBound, upperBound, nodeIdType, nodeMap)

    //record each iteration of non star query  unknown nodes traversal result
    var previousNonStarQueryRdd: RDD[(VertexId, (List[VertexId], Double))] = sc.emptyRDD[(VertexId, (List[VertexId], Double))]
    
    var topKKthLowerBoundScore = 0.0        //initialize for each candidate embedding enumeration traverse bounds
    var topKKthSmallestScore = -1.0     //0.0          // the smallest score in the candidate k list for candidate selection phase

    while (i < topKStarRstLstBuffer.length-1)      //  
    {
       //initialize the source list, as the new specificNodeIdLst
        val specificNodeIdLst = topKStarRstLstBuffer(i).map{x => (x._1, x._2._4, x._2._1)}         // dstId node becomes new's specific node,  => (nodeId, nodeTypeId, node matchingScore)
         .collect().toList.sortBy(x=>x._3)            // sorted by node matching score

        //val specificNodeIdLstHashMap = specificNodeIdLst.map{s => (s._1, (s._2, s._3))}

        //check the visited nodes to have the another unkown  destination candidates nodes; that is in the topKStarRstLst(i+1) 
        val nextUnknownDestNodeIdLst = topKStarRstLstBuffer(i+1).map{x => (x._1, x._2._4, x._2._1)}         // => (nodeId, nodeTypeId, node matchingScore)
           .collect().toList.sortBy(x=>x._3)            // sorted by node matching score
        
        print ("762 nonStarQueryGraphbfsTraverseTwoQueryNodes specificNodeIdLsts: " + specificNodeIdLst +  "nextUnknownDestNodeIdLst "+ nextUnknownDestNodeIdLst + " " + i + "\n")  
       //initialize from first topKStarQuery as specificNodeList
       var g: Graph[(VD, Map[VertexId, NodeInfo]), ED] =
       graph.mapVertices((id, nodeIdType) => (nodeIdType, 
                                              specificNodeIdLst.map(specificNodeIdType=> specificNodeIdType._1-> NodeInfo(specificNodeIdType._1, 
                                                                                                                          specificNodeIdType._2, if (id == specificNodeIdType._1) 0 else Long.MaxValue, if (id == specificNodeIdType._1) 1 else 0, 0.0, 0.0, 0, if (id == specificNodeIdType._1) GREY.id else WHITE.id, if (id == specificNodeIdType._1) 1.0 else 0.0, if (id == specificNodeIdType._1) 1.0 else 1.0)).toMap
       )).cache()

       val dstTypeId = dstTypeIdLst(i+1)              //updated the dstTypeId
       //var topKResultRdd: RDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))] = sc.emptyRDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))]           //Return Result RDD, (nodeId, matchingScore, lowerBound, upperBound, nodeMap)
                         
      var visitedDestinationRdd =  sc.emptyRDD[(VertexId, (VD, Map[VertexId, NodeInfo]))]      // RDD for next destination node visited
      var currentIterateTupleNodeResult = sc.emptyRDD[((VertexId, VertexId), Map[VertexId, NodeInfo])]         //Tuple result for general non-star query
      var allNodesVisitedNumber: Long = 0L                         // all nodes visited
      var oldAllNodesVisitedNumber: Long = -1L                     //previous iteration nodes visited
     // var twoPreviousOldAllNodesVisitedNumber: Long = -2L          //previous and previous iteration nodes visited
    
      var allNextDestNodesVisitedNumber = 0L            //all the next destNode visited
       //aggregate the message 
      while (nextUnknownDestNodeIdLst.size != allNextDestNodesVisitedNumber && oldAllNodesVisitedNumber != allNodesVisitedNumber && allNodesVisitedNumber < graph.ops.numVertices)
      {
         val msgs = g.aggregateMessages[(VD, Map[VertexId, NodeInfo],  Map[VertexId, (Double, Double)], Map[VertexId, Double], VertexId, VertexId)](
         triplet => {
           val srcNodeMap = triplet.srcAttr._2
           //println("266 starQueryGraphbfsTraverse srcNodeMap: ", srcNodeMap)
           var dstNodeMap = triplet.dstAttr._2
           var sendMsgFlag = false             //sendMsgFlag false
           var newdstNodeMap = Map[VertexId, NodeInfo]()            // not dstNodeMap any more, empty first, only updated vertexId part need to be sent (message)
           val dstNodeTypeId = triplet.dstAttr._1 
           
           var prevIterParentNodeLowerBoundsMap = Map[VertexId, (Double, Double)]()  
           var  prevIterCurrentNodeLowerBoundsMap = Map[VertexId, Double]()            //lower bound similarity score at previous iteration t-1
       
           specificNodeIdLst.foreach((specificNodeIdType: (VertexId, Int, Double)) => 
               if (dstNodeMap(specificNodeIdType._1).visitedColor != GREY.id && srcNodeMap(specificNodeIdType._1).visitedColor != RED.id && 
                   srcNodeMap(specificNodeIdType._1).spDistance != Long.MaxValue && srcNodeMap(specificNodeIdType._1).spDistance + 1  < dstNodeMap(specificNodeIdType._1).spDistance
                  && srcNodeMap(specificNodeIdType._1).closenessNodeScore > topKKthSmallestScore)
               {
                   val specificNodeId = specificNodeIdType._1
                   val specNodeIdType = specificNodeIdType._2 
                   
                   //check whether it's hierarchical relations
                   if (starQuery.getHierarchicalInheritance(specNodeIdType, dstTypeId, databaseType, hierarchialRelation)){

                     //update hierarchical level, spDist, spNumber, and parentId 
                      val changedEdgeLevel: Double = starQuery.BETA*math.abs(triplet.attr.toString.toInt)
                      
                      val tmpNodeInfo = dstNodeMap(specificNodeId).copy(spDistance = srcNodeMap(specificNodeId).spDistance+1, 
                                                         spNumber= srcNodeMap(specificNodeId).spNumber,
                                                         hierLevelDifference = srcNodeMap(specificNodeId).hierLevelDifference + changedEdgeLevel, parentId = triplet.srcId)  
                      //update dstNodeMap 
                      newdstNodeMap += (specificNodeId -> tmpNodeInfo) 
                      
                   }
                   else{
                      //update  spDist, spNumber, and parentId 
                      val tmpNodeInfo = dstNodeMap(specificNodeId).copy(spDistance = srcNodeMap(specificNodeId).spDistance+1, 
                                                          spNumber= srcNodeMap(specificNodeId).spNumber, 
                                                          hierLevelDifference = srcNodeMap(specificNodeId).hierLevelDifference, parentId = triplet.srcId)  
        
                      newdstNodeMap += (specificNodeId -> tmpNodeInfo) 
                   }
                   
                   //get current lowerbounds that is actually the previous iteration's lower bound  t-1
                   val neighborNodehierLevelDifference = starQuery.BETA*math.abs(triplet.attr.toString.toInt)              //edge hierarchical level difference between srcId and dstId
                   prevIterParentNodeLowerBoundsMap += (specificNodeIdType._1-> (srcNodeMap(specificNodeIdType._1).lowerBoundCloseScore, neighborNodehierLevelDifference))    //double check                   sendMsgFlag = true
                   prevIterCurrentNodeLowerBoundsMap += (specificNodeIdType._1-> dstNodeMap(specificNodeIdType._1).lowerBoundCloseScore)  //check
                   sendMsgFlag = true
               }
           )
           
           if (sendMsgFlag){
           // triplet.sendToDst((dstNodeTypeId, newdstNodeMap))
              triplet.sendToDst((dstNodeTypeId, newdstNodeMap, prevIterParentNodeLowerBoundsMap, prevIterCurrentNodeLowerBoundsMap, triplet.srcId, triplet.dstId))
              sendMsgFlag = false

           }
         },

          (a, b) => {      
            val nodeTypeId = a._1
            val nodeMapA = a._2
            val nodeMapB = b._2
            var prevIterParentNodeLowerBoundsMapA = a._3
            val prevIterParentNodeLowerBoundsMapB = b._3
          
            var prevIterCurrentLowerBoundsMapA = a._4              //Map[VertexId, Double]()
            val prevIterCurrentLowerBoundsMapB = b._4
            val srcId1 = a._5
            val srcId2 = b._5
            val dstId1 = a._6
            val dstId2 = b._6
          
            var newMap = Map[VertexId, NodeInfo]()
            var prevIterParentNodeLowerBoundsMapNew = Map[VertexId, (Double, Double)]() 
            var prevIterCurrentLowerBoundsMapNew = Map[VertexId, Double]()
          
            specificNodeIdLst.foreach{(specificNodeIdType: (VertexId, Int, Double)) =>
              
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

          (nodeTypeId, newMap, prevIterParentNodeLowerBoundsMapNew, prevIterCurrentLowerBoundsMapNew, srcId1, dstId1)
        
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
           val prevIterParentNodeLowerBoundsMap = newAttr._3       //Map[VertexId, Double]()       neighbor node parent node
           val prevIterCurrentNodeLowerBoundsMap = newAttr._4              //Map[VertexId, Double]()          current node's previous iteration'
              
           var newMap = nodeOldMap    //Map[VertexId, NodeInfo]()           //initialization 

           nodeNewMap.keys.foreach{(specificNodeId) =>
            
             if (nodeNewMap(specificNodeId).spDistance <= nodeOldMap(specificNodeId).spDistance)
             {
                  // println("464 starQueryGraphbfsTraverse: "  + specificNodeId + " nodeId: " + nodeId)
                  val spDistance = nodeNewMap(specificNodeId).spDistance
                  val spNumber = nodeNewMap(specificNodeId).spNumber
                  val newhierLevelDifference =  nodeNewMap(specificNodeId).hierLevelDifference        //(-1) x hierLevelDifference； downward inheritance

                  //update closeness score from this sepcific nodeId
                  val parentNodeHierarchicalLevel = newhierLevelDifference - prevIterParentNodeLowerBoundsMap(specificNodeId)._2         //parent node max hierarchical level difference
                  val newClosenessScore = starQuery.calculateClosenessScore(spDistance, spNumber, parentNodeHierarchicalLevel, newhierLevelDifference)      //node similarity score

                  //update lower bound score from this specific nodeId
                  // val updatedLowerBoundCloseScore = calculateLowerBound(specificNodeId, nodeNewMap, prevIterLowerBoundsMapA(specificNodeId))
                  val prevIterCurrentNodeLowerScore = prevIterCurrentNodeLowerBoundsMap(specificNodeId)
                  val parentNodePrevIterLowerScore = prevIterParentNodeLowerBoundsMap(specificNodeId)._1
                  val neighborNodehierLevelDifference = prevIterParentNodeLowerBoundsMap(specificNodeId)._2
                 // val parentNodeHierarchicalLeve = newhierLevelDifference - prevIterParentNodeLowerBoundsMap(specificNodeId)._2

                  val newLowerBoundCScore = starQuery.calculateNodeSimilarityLowerBound(spDistance, parentNodeHierarchicalLevel, prevIterCurrentNodeLowerScore,  parentNodePrevIterLowerScore, neighborNodehierLevelDifference)
                  val newUpperBoundCScore = starQuery.calculateNodeSimilarityUpperBound(newLowerBoundCScore, spDistance, newhierLevelDifference)

                  val tmpNodeInfo = nodeNewMap(specificNodeId).copy(visitedColor = GREY.id, spNumber = spNumber, hierLevelDifference = newhierLevelDifference, closenessNodeScore = newClosenessScore,
                                                                     lowerBoundCloseScore = newLowerBoundCScore, upperBoundCloseScore = newUpperBoundCScore)  //update closenessNodeScore  etc
                  newMap += (specificNodeId -> tmpNodeInfo)
                  
             }
          }

         (nodeTypeId, newMap)

        }.cache()

        val allNodesVisitedAnyOne = g.vertices.filter{ case x=>
           //judge the nodes is visited from any one of the specific nodes
           def getAnyVisitedFlag(nodeMap: Map[VertexId, NodeInfo]) ={             //define function
             var visitedFlag = false
             for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
                 if (nodeInfo.spDistance != Long.MaxValue)        //any one exist
                     visitedFlag = true
             }
             visitedFlag
           }
           getAnyVisitedFlag(x._2._2)         //nodeMap

         }

        //update bounds of each node in graph g  
        g = nonStarQuerySetnodeIdColorForBound(allNodesVisitedAnyOne, g, starQueryNodeHashMap, i, topKKthLowerBoundScore)                 //update bounding nodes color
        
        oldAllNodesVisitedNumber = allNodesVisitedNumber           //update previous as the current

        allNodesVisitedNumber = allNodesVisitedAnyOne.count()
        print ("329 nonStarQueryGraphbfsTraverseTwoQueryNodes allNodesVisitedNumber: " + allNodesVisitedNumber + " " + oldAllNodesVisitedNumber + "\n")  
        //twoPreviousOldAllNodesVisitedNumber = oldAllNodesVisitedNumber        //update the previous previous visited nodes number from previous nodes
        //val anotherUnknownDestNodeIdHashMap = nextUnknownDestNodeIdLst.map{s => (s._1, (s._2, s._3))}

        visitedDestinationRdd = allNodesVisitedAnyOne.filter{
           case x=> 

            def getVisitedDestFlag(inputNode: VertexId) = {
               var flag = false
               if (nextUnknownDestNodeIdLst.map(_._1).contains(inputNode))
               {
                 flag = true 
               }
               flag
            }

            getVisitedDestFlag(x._1)
         }

         //another termination condition: if all nextUnknownDestNodeIdLst are all visited, then the recycle termination
         //get the visited nodes for all the specific nodes and also in the next destination nodeId
        allNextDestNodesVisitedNumber =  visitedDestinationRdd.filter{ case x=>
            
           def getAllVisitedFlag(nodeMap: Map[VertexId, NodeInfo]) = {             //define function
             var visitedAllFlag = true
             for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
                 if (nodeInfo.spDistance == Long.MaxValue)          //any one exist
                     visitedAllFlag = false
             }
              visitedAllFlag
           }

            getAllVisitedFlag(x._2._2)   // nodeMap, nodeId
         }.count()

        print ("364 nonStarQueryGraphbfsTraverseTwoQueryNodes visitedDestinationRdd count: " + nonStarQuery_TOPK + " " +visitedDestinationRdd.count() + " "  + " " + allNextDestNodesVisitedNumber + " " + topKNonStarResultRdd.count() + "\n")  
        // visitedDestinationRdd.take(5).foreach(println)

        //update topKKthLowerBoundScore;        how?
        currentIterateTupleNodeResult = visitedDestinationRdd.map{
            case x=>

            def getVisitedCandidatesNodesTuple(destNodeId: VertexId, nodeMap: Map[VertexId, NodeInfo]) = {
              val candidateTupleLstBuffer = new ListBuffer[((VertexId, VertexId), Map[VertexId, NodeInfo])]()        //nodeId1, nodeId2, closeness score
              for ((specNodeId, nodeInfo) <- nodeMap){               //from every specific node
                if (nodeInfo.spDistance != Long.MaxValue)
                  {
                    candidateTupleLstBuffer += (((specNodeId, destNodeId), nodeMap))
                  }
              }
              candidateTupleLstBuffer              
            }

             getVisitedCandidatesNodesTuple(x._1, x._2._2)       //RDD's each element is a list
         }.flatMap(x => x)              

        if (currentIterateTupleNodeResult.count > nonStarQuery_TOPK)
        {
          
          val topKResultRddArray =  currentIterateTupleNodeResult.map(x=>
            (starQuery.calculateMatchingScoreLowerBound(x._2) + starQueryNodeHashMap((i, x._1._1)))                  // consider previous star query score result starQueryNodeHashMap 
          ).takeOrdered(nonStarQuery_TOPK)(Ordering[Double].on(x=>x))    //sort by matching score calculateNodeScoreStarquery result
       
          //get the kth smallest lower bound score in the topKResultRddArray
          topKKthLowerBoundScore = topKResultRddArray.head    //.sortBy(x=>x)

          //get smallest score in the nonStarQuery_TOPK more list
          topKKthSmallestScore = currentIterateTupleNodeResult.map(x=>         // ((specNodeId, destNodeId), nodeMap) 
           (x._2(x._1._1).closenessNodeScore)).takeOrdered(nonStarQuery_TOPK)(Ordering[Double].on(x=>x)).head
           
          //  println("395 nonStarQueryGraphbfsTraverseTwoQueryNodes topKKthLowerBoundScore tttt i: " + topKResultRddArray.toList +" " + topKKthLowerBoundScore + "\n") 

        }  
        
      }        // end aggregateMessage;  while (nextUnknownDestNodeIdLst.size != allNextDestNodesVisitedNumber ... 
      
      //begin the next iteration of next specific node
      //get the candidate pairs final matching score
      val count = i          //necessary, in case of parallelling, i has updated outside, but we dont need to the value i outside
      val currentNonStarResultRdd = currentIterateTupleNodeResult.map{x =>
         val specNodeId = x._1._1

         def getmatchingScoreTuple(specinodeId: VertexId) = {
            var starQueryPrevScoreSum = 0.0
            //println("nonStarQueryGraphbfsTraverseTwoQueryNodes ssssssssssssss i: " + i + " " + count +" " + specinodeId + "\n") 
            starQueryPrevScoreSum += starQueryNodeHashMap((count, specinodeId)) //candidateNodeRDD.filter(x => x._1 == nodeId).take(1).head._2
            starQueryPrevScoreSum += x._2(specinodeId).closenessNodeScore     
            
            starQueryPrevScoreSum            // only 2 here
        }
        (x._1._1, (x._1._2, getmatchingScoreTuple(specNodeId)))       //（specnodeId, (destNodeId, score))

      }       // .takeOrdered(nonStarQuery_TOPK)(Ordering[Double].reverse.on(x=>x._2))
     // println("418 nonStarQueryGraphbfsTraverseTwoQueryNodes ttttttttttttt i: " + i + " " + visitedDestinationRdd.count() + " " + currentIterateTupleNodeResult.count() + " " + currentNonStarResultRdd.count() + "\n") 
      //get the current top-k result
      if (0 == count)
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
             x =>                  //(specNodeId, ((nextNodeId, currentScore), (List(nodeId), prevscore))) 
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
             
    }     //end while (i < topKStarRstLstBuffer.length-1)  
    
    
    if (previousNonStarQueryRdd.count() < nonStarQuery_TOPK)
    {
        topKNonStarResultRdd = previousNonStarQueryRdd.map{ x =>
          (x._1::x._2._1, x._2._2 + starQueryNodeHashMap((i, x._1)))      ///(2*topKStarRstLstBuffer.length-1)
        }
    }
    else
    {
      val topKNonStarResultArray = previousNonStarQueryRdd.map{ x =>
          (x._1::x._2._1, x._2._2 + starQueryNodeHashMap((i, x._1)))        ///(2*topKStarRstLstBuffer.length-1)

        }.takeOrdered(nonStarQuery_TOPK)(Ordering[Double].reverse.on(x=>x._2))
      //print ("nonStarQueryGraphbfsTraverseTwoQueryNodes topKNonStarResultArray: " + nonStarQuery_TOPK + " " + topKNonStarResultArray.size + "\n")

      topKNonStarResultRdd = sc.parallelize(topKNonStarResultArray) 

    }

    //topKNonStarResultRdd.take(5).foreach(println)
    //println("nonStarQueryGraphbfsTraverseTwoQueryNodes ppppppppppreviousNonStarQueryRdd result i: ", previousNonStarQueryRdd.count()) 
    //previousNonStarQueryRdd. take(5).foreach(println)
    
    val endTime = System.currentTimeMillis()   
    println("482 only nonStarQueryGraphbfsTraverseQueryNodes runtime: "+ (endTime-startTime) + " ms") 
 
    topKNonStarResultRdd
    
  }

  
  //entry to execute general non star query 
  def nonStarQueryExecute[VD, ED](sc: SparkContext, graph: Graph[VD, ED], specificNodeIdTwoDimensionLst: List[List[(VertexId,Int)]], dstTypeIdLst: List[Int], nonStarQueryTOPK: Int, databaseType: Int, inputFileNodeInfoPath: String, outputFilePath: String, runTimeoutputFilePath: String, hierarchialRelation: Boolean) = {
      
    nonStarQuery_TOPK = nonStarQueryTOPK
    
    //phas1: decompose star query and execute star query
    var topKStarRstLst = new ListBuffer[RDD[(VertexId, (Double, Double, Double, Int, Map[VertexId, NodeInfo]))]]()
    var i = 0
    val startTime = System.currentTimeMillis()              //System.nanoTime()
    //print ("specificNodeIdTwoDimensionLst size: " + specificNodeIdTwoDimensionLst(0) + " " + i + " \n")
    while(i < specificNodeIdTwoDimensionLst.size)
    {
        val dstTypeId = dstTypeIdLst(i)
        //val topKResultRdd = starQuery.starQueryGraphbfsTraverseWithBoundPruning(sc, graph, specificNodeIdTwoDimensionLst(i), dstTypeId, databaseType, null, hierarchialRelation)
       
        //when return paths
        val answers = starQuery.starQueryGraphbfsTraverseWithBoundPruning(sc, graph, specificNodeIdTwoDimensionLst(i), dstTypeId, databaseType, null, hierarchialRelation)
        val topKResultRdd = answers._1
        val pathAnswerRdd = answers._2
        //topKResultRdd.take(5).foreach(println)
        //print ("i: " + specificNodeIdTwoDimensionLst(i) + " " + i + " \n")
        print ("star query " + i.toString + " pathAnswerRdd: " + " \n")
        pathAnswerRdd.take(5).foreach(println)

        topKStarRstLst  += topKResultRdd
        i = i + 1
    }
    
    //print ("size: " + topKStarRstLst.size +  " \n")
    //begin executing candiate selection phase
    // val topKNonStarResultRdd = nonStarQueryGraphbfsTraverseTwoQueryNodes(sc, graph, topKStarRstLst, dstTypeIdLst)
    val topKNonStarResultRdd = nonStarQueryGraphbfsTraverseAnyQueryNodesWithPruningBounds(sc, graph, topKStarRstLst, dstTypeIdLst, databaseType,  hierarchialRelation)
    
    val endTime = System.currentTimeMillis()   
    println("nonStarQueryExecute whole runtime: "+ (endTime-startTime) + " ms") 
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
