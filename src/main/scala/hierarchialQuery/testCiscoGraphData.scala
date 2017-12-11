/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.scala.hierarchialQuery

import org.apache.spark.SparkContext

object testCiscoGraphData {
  
  //product database execution -- main entry
  def executeProductDatabase(args: Array[String], sc: SparkContext) = {
    
    starQueryCiscoData(args, sc)
    
    //val outputFileNode = "/home/fubao/workDir/ResearchProjects/GraphQuerySearchRelatedPractice/SparkDistributedPractice/output/ciscoProduct/starQueryOutput/starQueryoutNode"
    //args [0] is TOPK number
    
    //val outputFilePath = "/home/fubao/workDir/ResearchProjects/GraphQuerySearchRelatedPractice/SparkDistributedPractice/output/ciscoProduct/starQueryOutput/starQueryoutPath"

    //test file
    //val inputAdjacencyListfilePath = "/home/fubao/workDir/ResearchProjects/GraphQuerySearchRelatedPractice/Data/testInput/test_smallGraphSpark"

    
    //read adjacency list to vertex edge RDD
    //val hierGraph = graphInputCommon.readAdjcencyListFile(sc, inputAdjacencyListfilePath)

    //graphInputCommon.test(sc)
        
    //starQuery.bfs(hierGraph, 1L, 15L)
    //starQuery.singleSourceGraphbfsTraverse(hierGraph, 1L, 1)
    //starQuery.starQueryGraphbfsTraverse(sc, hierGraph, List(1L, 5L), 1)
    
    //here is the main function entry for star query
    //experiment input list element (nodeId and type)
   
    //val dstTypeId = 0
   // val topK = args(0).toInt
   // val databaseType = 0
   // val runTimeFileIndex = args(1)
    
   //  starQuery.TOPK = topK
   // val runTimeoutputFilePath = "/home/fubao/workDir/ResearchProjects/GraphQuerySearchRelatedPractice/SparkDistributedPractice/output/ciscoProduct/starQueryOutput/starQueryoutRuntime" + runTimeFileIndex

    /*
    val querySpecificNodeNumber = args(2).toInt
    val specificReadLst = inputQueryRead.getQuerySpecifiNodesLst(sc, inputNodeInfoFile, querySpecificNodeNumber)
    
    println("specificReadLst: ", specificReadLst.size)
    starQuery.starQueryExeute(sc, hierGraph, specificReadLst, dstTypeId, databaseType, inputNodeInfoFile, outputFileNode, outputFilePath, runTimeoutputFilePath)     //execute star query
    */
   
   /*
    val specificReadLst = List((2020L, 1), (9573L,5))
   //val specificReadLst = List((8987L, 4), (8330L,1))
    // val specificReadLst = List((5817L, 1), (5737L,1))
    starQuery.starQueryExeute(sc, hierGraph, specificReadLst, dstTypeId, databaseType, inputNodeInfoFile,  outputFilePath, runTimeoutputFilePath)     //execute star query
   */
  
    /*
    val runTimeFileIndex = args(1)
   //for varing top-k
    val runTimeoutputFilePath = "/home/fubao/workDir/ResearchProjects/GraphQuerySearchRelatedPractice/SparkDistributedPractice/output/ciscoProduct/nonStarQueryOutput/varingTopKOneMachine/nonstarQueryoutRuntime" + runTimeFileIndex

    //for non Star query
    //first get star query specific lists, two dimensional for two unknown nodes 6106
    //val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((2020L, 1), (9573L,5)), List((5817L, 1), (5737L,1)))
    //val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((9014L, 4), (7266L,1)), List((7266L, 1), (9573L,5)))
     val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((8987L, 4), (8330L,1)), List((2020L, 1), (9573L,5)), List((5817L, 1), (5737L,1)))
     //val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((8987L, 4), (8330L,1)), List((2020L, 1), (9573L,5)), List((5817L, 1), (5737L,1)), List((7266L, 1), (9573L,5)), List((9014L, 4), (7266L,1)))
     
    var dstTypeIdLstBuffer: ListBuffer[Int] = new ListBuffer[(Int)]
    for (specNodeLst <- specNodelistStarQueryTwoDimension)
    {
         
        dstTypeIdLstBuffer += (0)
    }
    print ("main dstTypeIdLstBuffer： " + dstTypeIdLstBuffer + "\n")
    val nonStarQueryTOPK = starQuery.TOPK
    nonStarQuery.nonStarQueryExecute(sc, hierGraph, specNodelistStarQueryTwoDimension, dstTypeIdLstBuffer, nonStarQueryTOPK, databaseType, inputNodeInfoFile, null, runTimeoutputFilePath)     //execute star query
    */
   
    
    /*
    // test varying query size
    val runTimeFileIndex = args(1)

    val inputFileSpecificStarQueryPath = "/home/fubao/workDir/ResearchProjects/hierarchicalNetworkQuery/hierarchicalQueryPython/output/extractSubgraphQueryOutput/ciscoDataExtractQueryGraph"
    
    val allquerySizeLsts = inputQueryRead.getQuerySizeNumber(sc, inputFileSpecificStarQueryPath)
   
    //print ("main allquerySizeLsts： " + allquerySizeLsts + "\n")
    //for varing query graph size
    var runTimeoutputFilePath = "/home/fubao/workDir/ResearchProjects/GraphQuerySearchRelatedPractice/SparkDistributedPractice/output/ciscoProduct/nonStarQueryOutput/varingSpecificSizeOneMachine/" + "queryGraphSize"
    
    var i = 0 
    var tmpRunTimeoutputFilePath = ""
    for (specNodelistStarQueryTwoDimension <- allquerySizeLsts)
    {
      
      tmpRunTimeoutputFilePath = runTimeoutputFilePath

      var dstTypeIdLstBuffer: ListBuffer[Int] = new ListBuffer[(Int)]
      for (specNodeLst <- specNodelistStarQueryTwoDimension)
      {

          dstTypeIdLstBuffer += (0)
      }
      print ("main dstTypeIdLstBuffer： " + dstTypeIdLstBuffer + "\n")
      val nonStarQueryTOPK = starQuery.TOPK
      i += 1
      tmpRunTimeoutputFilePath = tmpRunTimeoutputFilePath + i.toString + "_top" + nonStarQueryTOPK.toString + "_counts"  + runTimeFileIndex
      nonStarQuery.nonStarQueryExecute(sc, hierGraph, specNodelistStarQueryTwoDimension, dstTypeIdLstBuffer, nonStarQueryTOPK, databaseType, inputNodeInfoFile, null, tmpRunTimeoutputFilePath)     //execute non star query
      
    }
    */
   
    //test varing graphData in dblp data
   // val graphSizeRatio = args(2).toInt
    //val hierarchialRelation = true
  //  testVaringGraphDataProduct( sc, topK, runTimeFileIndex,  graphSizeRatio, databaseType, hierarchialRelation)
    
    //test w/o or w/ hierarchical relations
   // val hierarchialRelation = false
  //  testHierarchicalRelationProductData (sc, topK, runTimeFileIndex, databaseType, hierarchialRelation)
    
  }
  
   def starQueryCiscoData(args: Array[String], sc: SparkContext) = {
    
    // val file = "hdfs://localhost:8070/testEdgeListFile2")
    //val file = "hdfs://192.168.0.52:8070/testEdgeListFile2"
    val inputEdgeListfilePath = "../../Data/ciscoDataGraph/ciscoDataGraphInfo1.0/edgeListPart1.0"
    val inputNodeInfoFilePath = "../../Data/ciscoDataGraph/ciscoDataGraphInfo1.0/nodeInfoPart1.0"
    
    //read edge list to graphX graph
    val hierGraph = graphInputCommon.readEdgeListFile(sc, inputEdgeListfilePath, inputNodeInfoFilePath, "\t")

    val dstTypeId = 0                    //0 hierarchical node   or 1
    val topK = args(0).toInt
    starQuery.TOPK = topK
    
    val databaseType = 0              //Cisco data graph database   0
    val runTimeFileIndex = args(1)
    
    val specificReadLst = List((3237L, 1), (5446L, 1))        // three or more query graph size
    
    val hierarchialRelation = true

    val outputFilePath = "../output/ciscoProduct/starQueryOutput/starOutputFilePath" + runTimeFileIndex
    val runTimeoutputFilePath = "../output/ciscoProduct/starQueryOutput/starQueryoutRuntime" + runTimeFileIndex
    starQuery.starQueryExeute(sc, hierGraph, specificReadLst, dstTypeId, databaseType, inputNodeInfoFilePath,  outputFilePath, runTimeoutputFilePath, hierarchialRelation)     //execute star query
    
 
   }
  
}

