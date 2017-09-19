/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.scala.hierarchialQuery

import org.apache.spark.rdd.RDD
//import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.graphx._

//import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators
import scala.util.MurmurHash
import scala.collection.mutable.ListBuffer

//main entry 
object QueryMain {
  
  def main(args: Array[String]) {
    
    val appIdName = "Graph query with hierarhcial relation"
    val conf = new SparkConf().setAppName(appIdName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer","24")               // Now it's 24 Mb of buffer by default instead of 0.064 Mb
      .set("spark.hadoop.validateOutputSpecs", "false")            // override output with the same path
    val sc = new SparkContext(conf)    //executeProductDatabase(args, sc)
    
    //executeProductDatabase(args, sc)
    executeDblpGraphData(args, sc)
    println("executeDblpGraphData: done")
    
  }
  
  //product database execution -- main entry
  def executeProductDatabase(args: Array[String], sc: SparkContext) = {
    
     // val file = "hdfs://localhost:8070/testEdgeListFile2")
   //val file = "hdfs://192.168.0.52:8070/testEdgeListFile2"
    //val inputfilePath = "/home/fubao/workDir/ResearchProjects/GraphQuerySearchRelatedPractice/Data/testInput/teshierarchicalAdjacencyList"
    
    val inputAdjacencyListfilePath = "/home/fubao/workDir/ResearchProjects/hierarchicalNetworkQuery/inputData/ciscoProductVulnerability/newCiscoGraphAdjacencyList"
    val inputNodeInfoFile = "/home/fubao/workDir/ResearchProjects/hierarchicalNetworkQuery/inputData/ciscoProductVulnerability/newCiscoGraphNodeInfo"
    
    //val outputFileNode = "/home/fubao/workDir/ResearchProjects/GraphQuerySearchRelatedPractice/SparkDistributedPractice/output/ciscoProduct/starQueryOutput/starQueryoutNode"
    //args [0] is TOPK number
    
    val outputFilePath = "/home/fubao/workDir/ResearchProjects/GraphQuerySearchRelatedPractice/SparkDistributedPractice/output/ciscoProduct/starQueryOutput/starQueryoutPath"

    //test file
    //val inputAdjacencyListfilePath = "/home/fubao/workDir/ResearchProjects/GraphQuerySearchRelatedPractice/Data/testInput/test_smallGraphSpark"

    
    //read adjacency list to vertex edge RDD
    val hierGraph = graphInputCommon.readAdjcencyListFile(sc, inputAdjacencyListfilePath)

    //graphInputCommon.test(sc)
        
    //starQuery.bfs(hierGraph, 1L, 15L)
    //starQuery.singleSourceGraphbfsTraverse(hierGraph, 1L, 1)
    //starQuery.starQueryGraphbfsTraverse(sc, hierGraph, List(1L, 5L), 1)
    
    //here is the main function entry for star query
    //experiment input list element (nodeId and type)
    
   
    val dstTypeId = 0
    val topK = args(0).toInt
    starQuery.TOPK = topK
    val databaseType = 0
    
   // val runTimeFileIndex = args(1)
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
    
   
    /*
    // test varying query size
    val runTimeFileIndex = args(1)

    val inputFileSpecificStarQueryPath = "/home/fubao/workDir/ResearchProjects/hierarchicalNetworkQuery/hierarchicalQueryPython/output/extractSubgraphOutput/ciscoDataExtractQueryGraph"
    
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
   
  }
  
    
  //dblp data base execute --main entry
  def executeDblpGraphData(args: Array[String], sc: SparkContext) = {
      
    
    val inputEdgeListfilePath = "../../Data/dblpParserGraph/output/finalOutput/newOutEdgeListFile.tsv"
    val inputNodeInfoFilePath = "../../Data/dblpParserGraph/output/finalOutput/newOutNodeNameToIdFile.tsv"
        
    //read edge list to graphX graph
    val hierGraph = graphInputCommon.readEdgeListFile(sc, inputEdgeListfilePath, inputNodeInfoFilePath, "\t")

    val dstTypeId = 1                     //1: people
    val topK = args(0).toInt
    starQuery.TOPK = topK
    
    val databaseType = 1              //DBLP database

    val runTimeFileIndex = args(1)

    //val specificReadLst = List((188470L, 3), (10821L,1))
    //val specificReadLst = List((189059L, 3), (10821L,1))
     //val specificReadLst = List((189015L, 3), (10821L,1))
    // val specificReadLst = List((188857L, 3))
    
    
    /*
    val outputFilePath = "../output/dblpData/starQueryOutput/starOutputFilePath" + runTimeFileIndex
    val runTimeoutputFilePath = "../output/dblpData/starQueryOutput/starQueryoutRuntime" + runTimeFileIndex
    starQuery.starQueryExeute(sc, hierGraph, specificReadLst, dstTypeId, databaseType, inputNodeInfoFilePath,  outputFilePath, runTimeoutputFilePath)     //execute star query
    */
  
    /*
    //start non-star query
    val runTimeoutputFilePath = "../output/dblpData/nonStarQueryOutput/nonStarQueryoutRuntime" + runTimeFileIndex
    val outputFilePath =  "../output/dblpData/nonStarQueryOutput/nonStarQueryOutputFilePath" + runTimeFileIndex
    val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((189059L, 3)), List((189086L, 3)))
    //val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((189059L, 3)), List((189086L, 3)),List((188857L, 3)))

    var dstTypeIdLstBuffer: ListBuffer[Int] = new ListBuffer[(Int)]
    for (specNodeLst <- specNodelistStarQueryTwoDimension)
    {
         
        dstTypeIdLstBuffer += (1)
    }
    print ("main dstTypeIdLstBuffer： " + dstTypeIdLstBuffer + "\n")
    val nonStarQueryTOPK = starQuery.TOPK
    nonStarQuery.nonStarQueryExecute(sc, hierGraph, specNodelistStarQueryTwoDimension, dstTypeIdLstBuffer, nonStarQueryTOPK, databaseType, inputNodeInfoFilePath, outputFilePath, runTimeoutputFilePath)     //execute star query
    
    */
    
    
    //begin topK K varing test
    /*
    val specificReadLst = List((189015L, 3), (10821L,1))
    val runTimeoutputFilePath = "../output/dblpData/starQueryOutput/varingTopKOneMachine/starQueryoutRuntime" + runTimeFileIndex
    starQuery.starQueryExeute(sc, hierGraph, specificReadLst, dstTypeId, databaseType, inputNodeInfoFilePath,  null, runTimeoutputFilePath)     //execute star query
    */
   
    //val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((189059L, 3), (10821L,1)), List((189059L, 3), (189086L, 3)))
    //val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((189059L, 3), (189086L, 3)))

    /* 
    val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((189059L, 3), (10821L,1)), List((189059L, 3), (189086L, 3)), List((188857L, 3), (189086L, 3)))
    val runTimeoutputFilePath = "../output/dblpData/nonStarQueryOutput/varingTopKOneMachine/nonStarQueryoutRuntime" + runTimeFileIndex
    val outputFilePath = null      //"../output/dblpData/nonStarQueryOutput/varingTopKOneMachine/nonStarQueryOutputFilePath" + runTimeFileIndex

    var dstTypeIdLstBuffer: ListBuffer[Int] = new ListBuffer[(Int)]
    for (specNodeLst <- specNodelistStarQueryTwoDimension)
    {
         
        dstTypeIdLstBuffer += (1)
    }
    
    val nonStarQueryTOPK = starQuery.TOPK
    nonStarQuery.nonStarQueryExecute(sc, hierGraph, specNodelistStarQueryTwoDimension, dstTypeIdLstBuffer, nonStarQueryTOPK, databaseType, inputNodeInfoFilePath, outputFilePath, runTimeoutputFilePath)     //execute star query
    
    */
  
    //begin testing varying graph query size
    val inputFileSpecificStarQueryPath = "../../Data/extractSubgraph/output/extractDblpQuerySizeGraph/dblpDataExtractQueryGraph.tsv"
    
    val allquerySizeLsts = inputQueryRead.getQuerySizeNumber(sc, inputFileSpecificStarQueryPath)         //read query 
    val runTimeOutputFilePath = "../output/dblpData/nonStarQueryOutput/varyingQueryGraphSize_singleMachine/nonStarQueryOutRuntime" + runTimeFileIndex

    
    var i = 0 
    var tmpRunTimeOutputFilePath = ""
    //set destination noe type
    for (specNodelistStarQueryTwoDimension <- allquerySizeLsts)
    {
      
      tmpRunTimeOutputFilePath = runTimeOutputFilePath

      var dstTypeIdLstBuffer: ListBuffer[Int] = new ListBuffer[(Int)]
      for (specNodeLst <- specNodelistStarQueryTwoDimension)
      {

          dstTypeIdLstBuffer += (1)
      }
      
      print ("main  dblp dstTypeIdLstBuffer： " + dstTypeIdLstBuffer + "\n")
      val nonStarQueryTOPK = starQuery.TOPK
      i += 1
      tmpRunTimeOutputFilePath = tmpRunTimeOutputFilePath + i.toString + "_top" + nonStarQueryTOPK.toString + "_counts"  + runTimeFileIndex
      nonStarQuery.nonStarQueryExecute(sc, hierGraph, specNodelistStarQueryTwoDimension, dstTypeIdLstBuffer, nonStarQueryTOPK, databaseType, inputNodeInfoFilePath, null, tmpRunTimeOutputFilePath)     //execute non star query
    
    
    }
   
    
    
  }
  
  
  def testVaringGraphData () = {
    
    //test data graph size changing
    varingGraphRatio = 0.1
    val inputDir = "../output/extractSubgraph/output/dblpDataGraphExtractOut/dataGraphEdgeList0.1"  
    
    
  }
  
  
  
   
}