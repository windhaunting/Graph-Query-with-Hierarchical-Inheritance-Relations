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
    
    val hierarchialRelation = true
    
    // testSyntheticGraph.testTinyGraphData(args, sc)
    // println("testSyntheticGraph: done")
    
   
    testSyntheticGraph.executeSyntheticDatabase(args, sc, hierarchialRelation)
    println("executeSyntheticDatabase: done") 
    
   
   // testCiscoGraphData.executeProductDatabase(args, sc, hierarchialRelation)
   //  println("executeProductDatabase: done") 
    
   // testDblpGraphData.executeDblpGraphData(args, sc, hierarchialRelation)
   // println("executeDblpGraphData: done")
   
    
  }
  
  
  
  //function for testing varing graphData in dblp data
  def testVaringGraphDataDblp (sc: SparkContext, topK: Int, runTimeFileIndex: String, graphSizeRatio: Int, databaseType: Int, hierarchialRelation: Boolean) = {
    
    //test data graph size changing
    val varingGraphRatio = graphSizeRatio*0.1
    
    val inputNodeInfoFilePath = "../../Data/extractSubgraph/output/dblpDataGraphExtractOut/dataGraphInfo" +
                     varingGraphRatio.toString + "/nodeInfoPart" + varingGraphRatio.toString
    
    val inputEdgeListfilePath = "../../Data/extractSubgraph/output/dblpDataGraphExtractOut/dataGraphInfo" +
                     varingGraphRatio.toString + "/edgeListPart" + varingGraphRatio.toString
    
    val hierGraph = graphInputCommon.readEdgeListFile(sc, inputEdgeListfilePath, inputNodeInfoFilePath, "\t")
    
    print (" varingGraphRatio： " + varingGraphRatio + " " + inputEdgeListfilePath+ "\n")
    val runTimeOutputFilePath = "../output/dblpData/nonStarQueryOutput/varingDataGraphSizeOneMachine/nonStarQueryOutRuntime" + runTimeFileIndex

    val outputFilePath = null
    //val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((189015L, 3), (10821L, 1)))
    //val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((59897L, 3), (66520L, 2)))
    //val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((59897L, 2), (66520L,2)), List((54314L, 2), (66488L, 2)))
    val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((59897L, 2)), List((66520L,2), (123641L, 2)), List((59897L, 2)))
    
    var dstTypeIdLstBuffer: ListBuffer[Int] = new ListBuffer[(Int)]
    
    val nonStarQueryTOPK = topK
    for (specNodeLst <- specNodelistStarQueryTwoDimension)
    {
        dstTypeIdLstBuffer += (1)
    }
    
    
    print ("main dstTypeIdLstBuffer： " + dstTypeIdLstBuffer + "\n")
  //  nonStarQuery.nonStarQueryExecute(sc, hierGraph, specNodelistStarQueryTwoDimension, dstTypeIdLstBuffer, nonStarQueryTOPK, databaseType, inputNodeInfoFilePath, outputFilePath, runTimeOutputFilePath, hierarchialRelation)     //execute star query
    
  
  }
  
 
  //function for testing varing graphData in cisco product data
  def testVaringGraphDataProduct (sc: SparkContext, topK: Int, runTimeFileIndex: String, graphSizeRatio: Int, databaseType: Int, hierarchialRelation: Boolean) = {
    
    //test data graph size changing
    val varingGraphRatio = graphSizeRatio*0.1
    
    val inputNodeInfoFilePath = "../../../hierarchicalNetworkQuery/hierarchicalQueryPython/output/ciscoProductDataGraphExtractOut/dataGraphInfo" +
                     varingGraphRatio.toString + "/nodeInfoPart" + varingGraphRatio.toString
                     
    val inputEdgeListfilePath = "../../../hierarchicalNetworkQuery/hierarchicalQueryPython/output/ciscoProductDataGraphExtractOut/dataGraphInfo" +
                     varingGraphRatio.toString + "/edgeListPart" + varingGraphRatio.toString
    
    val hierGraph = graphInputCommon.readEdgeListFile(sc, inputEdgeListfilePath, inputNodeInfoFilePath, "\t")
    print (" varingGraphRatio： " + varingGraphRatio + " " + inputEdgeListfilePath+ "\n")
    val runTimeOutputFilePath = "../output/ciscoProduct/nonStarQueryOutput/varingDataGraphSizeOneMachine/nonStarQueryOutRuntime" + runTimeFileIndex
    val outputFilePath = null
    
    //val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((9104L, 5), (6145L, 1)))
    //val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((9104L, 5), (6145L, 1)), List((10210L, 6), (9923L, 6)))
    val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((9104L, 5), (6145L, 1)), List((10210L, 6), (9923L, 6)), List((11300L, 6), (11095L, 6)))

    var dstTypeIdLstBuffer: ListBuffer[Int] = new ListBuffer[(Int)]
    
    val nonStarQueryTOPK = topK
    for (specNodeLst <- specNodelistStarQueryTwoDimension)
    {
        if (databaseType == 0){
            dstTypeIdLstBuffer += (0)                  //product type
        }
        else{
            dstTypeIdLstBuffer += (1)                  //people type here
        }
    }
    
    print ("main dstTypeIdLstBuffer： " + dstTypeIdLstBuffer + "\n")
  //  nonStarQuery.nonStarQueryExecute(sc, hierGraph, specNodelistStarQueryTwoDimension, dstTypeIdLstBuffer, nonStarQueryTOPK, databaseType, inputNodeInfoFilePath, outputFilePath, runTimeOutputFilePath, hierarchialRelation)     //execute star query
    
    
  }
   
  
  //test theresult w/o and w/ hierarchical relations in product data
  def testHierarchicalRelationProductData (sc: SparkContext, topK: Int, runTimeFileIndex: String, databaseType: Int, hierarchialRelation: Boolean) = {
   //based on star query check
   val inputEdgeListfilePath = "../../../hierarchicalNetworkQuery/hierarchicalQueryPython/output/ciscoProductDataGraphExtractOut/dataGraphInfo1.0/edgeListPart1.0"      //"../../../hierarchicalNetworkQuery/inputData/ciscoProductVulnerability/newCiscoGraphAdjacencyList"
   val inputNodeInfoFilePath = "../../../hierarchicalNetworkQuery/hierarchicalQueryPython/output/ciscoProductDataGraphExtractOut/dataGraphInfo1.0/nodeInfoPart1.0"
    
    /*
    var outputFilePath = ""
    if (hierarchialRelation){
        outputFilePath = "../output/ciscoProduct/starQueryOutput/testWithOrWORelations/testWithHierarchiOutput" + runTimeFileIndex + ".tsv"   
    }
    else{
        outputFilePath = "../output/ciscoProduct/starQueryOutput/testWithOrWORelations/testNoHierarchiOutput" + runTimeFileIndex + ".tsv"       
    }

    //read adjacency list to vertex edge RDD
    val runTimeoutputFilePath = null       //"../output/ciscoProduct/starQueryOutput/testWithOrWORelations/runTime" + runTimeFileIndex
    val hierGraph = graphInputCommon.readEdgeListFile(sc, inputEdgeListfilePath, inputNodeInfoFilePath, "\t")
    val specificReadLst = List((8330L, 1), (8987L,4))
    val dstTypeId = 0
    
    starQuery.starQueryExeute(sc, hierGraph, specificReadLst, dstTypeId, databaseType, inputNodeInfoFilePath,  outputFilePath, runTimeoutputFilePath, hierarchialRelation)     //execute star query
    
    */
   
    //test runtime with different query size
    val hierGraph = graphInputCommon.readEdgeListFile(sc, inputEdgeListfilePath, inputNodeInfoFilePath, "\t")
    val inputFileSpecificStarQueryPath = "../../../hierarchicalNetworkQuery/hierarchicalQueryPython/output/extractSubgraphQueryOutput/ciscoDataExtractQueryGraph"
    
    val allquerySizeLsts = inputQueryRead.getQuerySizeNumber(sc, inputFileSpecificStarQueryPath)
   
    //print ("main allquerySizeLsts： " + allquerySizeLsts + "\n")
    //for varing query graph size
    var runTimeoutputFilePath = ""
    if (hierarchialRelation){
        runTimeoutputFilePath = "../output/ciscoProduct/nonStarQueryOutput/testWithHierarchiOutputVaryingQuerySize/" + "queryGraphSize"
    }
    else{
        runTimeoutputFilePath = "../output/ciscoProduct/nonStarQueryOutput/testNoHierarchiOutputVaryingQuerySize/" + "queryGraphSize"
    }
    
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
    //  nonStarQuery.nonStarQueryExecute(sc, hierGraph, specNodelistStarQueryTwoDimension, dstTypeIdLstBuffer, nonStarQueryTOPK, databaseType, inputNodeInfoFilePath, null, tmpRunTimeoutputFilePath, hierarchialRelation)     //execute non star query
      
    }
    
  }
  
  
  
  //test the result w/o and w/ hierarchical relations in dblp data
  def testHierarchicalRelationDblpData (sc: SparkContext, topK: Int, runTimeFileIndex: String, databaseType: Int, hierarchialRelation: Boolean) = {
   //based on star query check    
    val inputEdgeListfilePath = "../../Data/dblpParserGraph/output/finalOutput/newOutEdgeListFile.tsv"
    val inputNodeInfoFilePath = "../../Data/dblpParserGraph/output/finalOutput/newOutNodeNameToIdFile.tsv"

    val hierGraph = graphInputCommon.readEdgeListFile(sc, inputEdgeListfilePath, inputNodeInfoFilePath, "\t")

    /*
    var outputFilePath = ""
    if (hierarchialRelation){
        outputFilePath = "../output/dblpData/starQueryOutput/testWithOrWORelations/testWithHierarchiOutput" + runTimeFileIndex + ".tsv"   
    }
    else{
        outputFilePath = "../output/dblpData/starQueryOutput/testWithOrWORelations/testNoHierarchiOutput" + runTimeFileIndex + ".tsv"       
    }

    //read adjacency list to vertex edge RDD
    val runTimeoutputFilePath = null       //"../output/ciscoProduct/starQueryOutput/testWithOrWORelations/runTime" + runTimeFileIndex
    //val specificReadLst = List((188856L, 3), (9136L,1))
    val specificReadLst = List((189009L, 3), (9136L,1))
    
    val dstTypeId = 1
    
    starQuery.starQueryExeute(sc, hierGraph, specificReadLst, dstTypeId, databaseType, inputNodeInfoFilePath,  outputFilePath, runTimeoutputFilePath, hierarchialRelation)     //execute star query
    */
    
    val inputFileSpecificStarQueryPath = "../../Data/extractSubgraph/output/extractDblpQuerySizeGraph/dblpDataExtractQueryGraph.tsv"
    
    val allquerySizeLsts = inputQueryRead.getQuerySizeNumber(sc, inputFileSpecificStarQueryPath)         //read query 
    var runTimeoutputFilePath = ""        //"../output/dblpData/nonStarQueryOutput/varyingQueryGraphSize_singleMachine/nonStarQueryOutRuntime" + runTimeFileIndex

    if (hierarchialRelation){
        runTimeoutputFilePath = "../output/dblpData/nonStarQueryOutput/testWithHierarchiOutputVaryingQuerySize/" + "queryGraphSize"
    }
    else{
        runTimeoutputFilePath = "../output/dblpData/nonStarQueryOutput/testNoHierarchiOutputVaryingQuerySize/" + "queryGraphSize"
    }
    
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
      
    }
      
  
  }
   
    
}