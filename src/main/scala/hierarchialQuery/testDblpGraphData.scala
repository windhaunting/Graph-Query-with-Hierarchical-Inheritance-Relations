/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.scala.hierarchialQuery

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object testDblpGraphData {
  
  
//dblp data base execute --main entry
  def executeDblpGraphData(args: Array[String], sc: SparkContext, hierarchialRelation: Boolean) = {
      
   
    val inputEdgeListfilePath = "../../Data/dblpParserGraph/output/finalOutput/newOutEdgeListFile.tsv"
    val inputNodeInfoFilePath = "../../Data/dblpParserGraph/output/finalOutput/newOutNodeNameToIdFile.tsv"
        
    //read edge list to graphX graph
    val hierGraphRdd = graphInputCommon.readEdgeListFile(sc, inputEdgeListfilePath, inputNodeInfoFilePath, "\t")

    starQueryDblpData(args, sc, hierGraphRdd, inputNodeInfoFilePath, hierarchialRelation)
    
    val inputGeneralQueryGraph = "../../Data/dblpParserGraph/output/inputDblpQueryGraph/generalQueryGraph/generateQuerygraphInput"

    // executeGeneralQueryDblpDatabase(args, sc, hierGraphRdd, inputGeneralQueryGraph, inputNodeInfoFilePath, hierarchialRelation)

    // executeGeneralQueryDblpDatabaseDifferentTopK(args, sc, hierGraphRdd, inputGeneralQueryGraph, inputNodeInfoFilePath, hierarchialRelation)
    
    // executeGeneralQueryDblpDatabaseDifferentQuerySize(args, sc, hierGraphRdd, inputGeneralQueryGraph, inputNodeInfoFilePath, hierarchialRelation)
    
    
     /*
     // test different data graph
     val dataGraphPathPrefix = "../../../hierarchicalNetworkQuery/extractSubgraph/output/dblpDataGraphExtractOut/dataGraphInfo"
     val inputGeneralQueryGraphPrefix = "../../../hierarchicalNetworkQuery/extractSubgraph/output/dblpDataGraphExtractOut/inputGeneralQueryGraph/queryGraphInput"
     
     executeGeneralQueryDblpDatabaseDifferentDataSize(args, sc, dataGraphPathPrefix, inputGeneralQueryGraphPrefix, hierarchialRelation)
     */
    
   
  }
  
  //entry for star query for dblp data
  def starQueryDblpData[VD, ED](args: Array[String], sc: SparkContext, dataGraph: Graph[VD, ED], inputNodeInfoFilePath: String, hierarchialRelation: Boolean) = {
      
    val dstTypeId = 1                    //0 hierarchical node   or 1
    val topK = args(0).toInt
    starQuery.TOPK = topK
    
    val databaseType = 1              //dblp data graph database   1
    val runTimeFileIndex = args(1)
    
    val specificReadLst = List((188421L, 3), (188806L, 3))        // three or more query graph size
    
   // val specificReadLst = List((188912L, 3), (188400L, 3), (188914L, 3))        // three or more query graph size
    val hierarchialRelation = true
   
    val outputFilePath = "../output/dblpData/starQueryOutput/starQueryOutputFilePath" + runTimeFileIndex
    val runTimeoutputFilePath = "../output/dblpData/starQueryOutput/starQueryoutRuntime" + runTimeFileIndex
    starQuery.starQueryExeute(sc, dataGraph, specificReadLst, dstTypeId, databaseType, inputNodeInfoFilePath,  outputFilePath, runTimeoutputFilePath, hierarchialRelation)     //execute star query
    
  
 }
  //entry for generic graph query for dblp Data
  def executeGeneralQueryDblpDatabase[VD, ED](args: Array[String], sc: SparkContext, dataGraph: Graph[VD, ED], inputGeneralQueryGraph: String, inputNodeInfoFilePath: String, hierarchialRelation: Boolean) = {
    val allquerySizeLsts = inputQueryRead.getDecomposedStarQuerySpecificNodes(sc, inputGeneralQueryGraph)
    
    val topK = args(0).toInt      //topK
    starQuery.TOPK = topK
    val databaseType = 1             //dblp graph database   1
    
    val runTimeFileIndex = args(1)
    
    print ("executeGeneralQueryDblpDatabase main allquerySizeLsts： " + allquerySizeLsts + "\n")
    //for varing query graph size
    var runTimeOutputFilePath = ""
    var outputResultFilePath = ""
    if (hierarchialRelation){
        runTimeOutputFilePath = "../output/dblpData/nonStarQueryOutput/testWithHierarchiQueryOutput/" + "queryRuntime"
        outputResultFilePath = "../output/dblpData/nonStarQueryOutput/testWithHierarchiQueryOutput/" + "runResult"
    }
    else{
        runTimeOutputFilePath = "../output/dblpData/nonStarQueryOutput/testWOHierarchiQueryOutput/" + "queryRuntime"
        outputResultFilePath = "../output/dblpData/nonStarQueryOutput/testWOHierarchiQueryOutput/" + "runResult"
    }
    
    var count = 1
    for (specNodelistStarQueryLst <- allquerySizeLsts)
    {
       //print ("executeGeneralQuerySyntheticDatabase specNodelistStarQueryLst： " + specNodelistStarQueryLst + "\n")
       val starQueryNodeLst = specNodelistStarQueryLst._1
       val dstTypeLst = specNodelistStarQueryLst._2

      print ("executeGeneralQueryDblpDatabase starQueryNodeLst： " + starQueryNodeLst + " " + dstTypeLst+  "\n")
      val nonStarQueryTOPK = starQuery.TOPK

      //general query 
      runTimeOutputFilePath = runTimeOutputFilePath + count.toString + "_top" + nonStarQueryTOPK.toString + "_times" + runTimeFileIndex
      outputResultFilePath = outputResultFilePath + count.toString + "_top" + nonStarQueryTOPK.toString + "_times" + runTimeFileIndex
      
      //general non-star query execution
      nonStarQuery.nonStarQueryExecute(sc, dataGraph, starQueryNodeLst, dstTypeLst, nonStarQueryTOPK, databaseType, inputNodeInfoFilePath, outputResultFilePath, runTimeOutputFilePath, hierarchialRelation)     //execute non star query
      count += 1
    }
  
  }
  
  
  // varing differentTopK test;  general general query entry (non-star query) for cisco data graph
  def executeGeneralQueryDblpDatabaseDifferentTopK[VD, ED](args: Array[String], sc: SparkContext, dataGraph: Graph[VD, ED], inputGeneralQueryGraph: String, inputNodeInfoFilePath: String, hierarchialRelation: Boolean) = {
 
    val allquerySizeLsts = inputQueryRead.getDecomposedStarQuerySpecificNodes(sc, inputGeneralQueryGraph)
    //val topK = args(0).toInt             //topK
    //starQuery.TOPK = topK
    val databaseType = 1                  //synthetic graph database   2
    
    val runTimeFileIndex = args(0)           
    
    print ("main allquerySizeLsts： " + allquerySizeLsts + "\n")
    //for varing query graph size
    var runTimeOutputFilePathOrigin = ""
    var outputResultFilePathOrigin = ""
    if (hierarchialRelation){
        runTimeOutputFilePathOrigin = "../output/dblpData/nonStarQueryOutput/testWithHierarchiQueryOutput/" + "queryRuntime"
        outputResultFilePathOrigin = "../output/dblpData/nonStarQueryOutput/testWithHierarchiQueryOutput/" + "runResult"
    }
    else{
        runTimeOutputFilePathOrigin = "../output/dblpData/nonStarQueryOutput/testWOHierarchiQueryOutput/" + "queryRuntime"
        outputResultFilePathOrigin = "../output/dblpData/nonStarQueryOutput/testWOHierarchiQueryOutput/" + "runResult"
    }
    
    var count = 1
    val varingTokList = List(1, 2, 5, 10, 15, 20, 25, 30)   //List(1)       // List(1, 2, 5, 10, 15, 20, 25, 30)       //  List(1)
    for (specNodelistStarQueryLst <- allquerySizeLsts)
    {
       //print ("executeGeneralQuerySyntheticDatabase specNodelistStarQueryLst： " + specNodelistStarQueryLst + "\n")
       val starQueryNodeLst = specNodelistStarQueryLst._1
       val dstTypeLst = specNodelistStarQueryLst._2
        print ("starQueryNodeLst： " + starQueryNodeLst + " dstTypeLst: " + dstTypeLst+ " nonStarQueryTOPK:  " + varingTokList +"\n")

      for(topk <- varingTokList) {
          starQuery.TOPK = topk
          val nonStarQueryTOPK = starQuery.TOPK

          //general query 
          val runTimeOutputFilePath = runTimeOutputFilePathOrigin  + "_top" + nonStarQueryTOPK.toString + "_queryGRaphSizeNo" + count.toString + "_" + runTimeFileIndex
          val outputResultFilePath = outputResultFilePathOrigin  + "_top" + nonStarQueryTOPK.toString + "_queryGRaphSizeNo" + count.toString + "_" + runTimeFileIndex

          //general non-star query execution
          nonStarQuery.nonStarQueryExecute(sc, dataGraph, starQueryNodeLst, dstTypeLst, nonStarQueryTOPK, databaseType, inputNodeInfoFilePath, outputResultFilePath, runTimeOutputFilePath, hierarchialRelation)     //execute non star query
      }
      
      count += 1
    }
        
  }
  
  
  
  // varing different query graph size test;  general general query entry (non-star query) for synthetic graph
  def executeGeneralQueryDblpDatabaseDifferentQuerySize[VD, ED](args: Array[String], sc: SparkContext, dataGraph: Graph[VD, ED], inputGeneralQueryGraph: String, inputNodeInfoFilePath: String, hierarchialRelation: Boolean) = {
 
    val allquerySizeLsts = inputQueryRead.getDecomposedStarQuerySpecificNodes(sc, inputGeneralQueryGraph)
    //val topK = args(0).toInt      //topK
    //starQuery.TOPK = topK
    val databaseType = 1              //synthetic graph database   2
    
    val runTimeFileIndex = args(0)           
    
    print ("main allquerySizeLsts： " + allquerySizeLsts.size + "\n")
    //for varing query graph size
    var runTimeOutputFilePathOrigin = ""
    var outputResultFilePathOrigin = ""
    if (hierarchialRelation){
        runTimeOutputFilePathOrigin = "../output/dblpData/nonStarQueryOutput/testWithHierarchiQueryOutput/varyingQueryGraphSizeOneMachine/" + "queryRuntime"
        outputResultFilePathOrigin = "../output/dblpData/nonStarQueryOutput/testWithHierarchiQueryOutput/varyingQueryGraphSizeOneMachine/" + "runResult"
    }
    else{
        runTimeOutputFilePathOrigin = "../output/dblpData/nonStarQueryOutput/testWOHierarchiQueryOutput/" + "queryRuntime"
        outputResultFilePathOrigin = "../output/dblpData/nonStarQueryOutput/testWOHierarchiQueryOutput/" + "runResult"
    }
    
    val varingTokList = List(2, 5, 10)   //List(1)   //List(2, 5, 10)       
    
    for(topk <- varingTokList) {
        starQuery.TOPK = topk
        val nonStarQueryTOPK = starQuery.TOPK
        
        var queryGraphSizeCount = 1

        for (specNodelistStarQueryLst <- allquerySizeLsts)
        {
           //print ("executeGeneralQuerySyntheticDatabase specNodelistStarQueryLst： " + specNodelistStarQueryLst + "\n")
           val starQueryNodeLst = specNodelistStarQueryLst._1
           val dstTypeLst = specNodelistStarQueryLst._2
           print ("starQueryNodeLst： " + starQueryNodeLst + " dstTypeLst: " + dstTypeLst+ " nonStarQueryTOPK:  " + topk +"\n")
         
          //general query 
          val runTimeOutputFilePath = runTimeOutputFilePathOrigin  + "_top" + nonStarQueryTOPK.toString + "_varyingQueryGRaphSizeNo" + queryGraphSizeCount.toString + "_" + runTimeFileIndex
          val outputResultFilePath = outputResultFilePathOrigin  + "_top" + nonStarQueryTOPK.toString + "_varyingQueryGRaphSizeNo" + queryGraphSizeCount.toString + "_" + runTimeFileIndex

          //general non-star query execution
          nonStarQuery.nonStarQueryExecute(sc, dataGraph, starQueryNodeLst, dstTypeLst, nonStarQueryTOPK, databaseType, inputNodeInfoFilePath, outputResultFilePath, runTimeOutputFilePath, hierarchialRelation)     //execute non star query
          queryGraphSizeCount += 1
        }
      }
  }
  
  
  
   // varing different data graph 10%, 20%, 50%, 80%, 100%;  genera query entry (non-star query) for dblp graph
  def executeGeneralQueryDblpDatabaseDifferentDataSize[VD, ED](args: Array[String], sc: SparkContext, inputDataGraphPathPrefix: String, inputGeneralQueryGraphPrefix: String, hierarchialRelation: Boolean) = {
 
     //val topK = args(0).toInt      //topK
     //starQuery.TOPK = topK
     val databaseType = 1              //synthetic graph database   2
    
     val runTimeFileIndex = args(0)           
    
    //for varing query graph size
    var runTimeOutputFilePathOrigin = ""
    var outputResultFilePathOrigin = ""
    if (hierarchialRelation){
        runTimeOutputFilePathOrigin = "../output/dblpData/nonStarQueryOutput/testWithHierarchiQueryOutput/varyingDataGraphSizeOneMachine/" + "queryRuntime"
        outputResultFilePathOrigin = "../output/dblpData/nonStarQueryOutput/testWithHierarchiQueryOutput/varyingDataGraphSizeOneMachine/" + "runResult"
    }
    else{
        runTimeOutputFilePathOrigin = "../output/dblpData/nonStarQueryOutput/testWOHierarchiQueryOutput/varyingDataGraphSizeOneMachine/" + "queryRuntime"
        outputResultFilePathOrigin = "../output/dblpData/nonStarQueryOutput/testWOHierarchiQueryOutput/varyingDataGraphSizeOneMachine/" + "runResult"
    }
    
     starQuery.TOPK = 5
     val nonStarQueryTOPK = starQuery.TOPK
    
     val subfixs =  List("0.1", "0.2", "0.5", "0.8", "1.0") // List("0.5", "0.8", "1.0")   // List("0.1", "0.2", "0.5", "0.8", "1.0") //List("0.1")  //
     
     for (subfix <- subfixs)
     {
        val inputEdgeListfilePathTmp =  inputDataGraphPathPrefix  + subfix + "/edgeListPart" + subfix
        val inputNodeInfoFilePathTmp = inputDataGraphPathPrefix  + subfix + "/nodeInfoPart" + subfix
        
        val hierGraph = graphInputCommon.readEdgeListFile(sc, inputEdgeListfilePathTmp, inputNodeInfoFilePathTmp, "\t")

        val inputGeneralQueryGraphPath = inputGeneralQueryGraphPrefix + subfix
        val allquerySizeLsts = inputQueryRead.getDecomposedStarQuerySpecificNodes(sc, inputGeneralQueryGraphPath)

      
        var queryGraphSizeCount = 1
        for (specNodelistStarQueryLst <- allquerySizeLsts)
        {
           //print ("executeGeneralQuerySyntheticDatabase specNodelistStarQueryLst： " + specNodelistStarQueryLst + "\n")
           val starQueryNodeLst = specNodelistStarQueryLst._1
           val dstTypeLst = specNodelistStarQueryLst._2
           print ("starQueryNodeLst： " + starQueryNodeLst + " dstTypeLst: " + dstTypeLst+ " dataGraphsubfix:  " + subfix +"\n")
         
          //general query 
          val runTimeOutputFilePath = runTimeOutputFilePathOrigin  + "_dataGraphsubfix" + subfix.toString + "_varyingDataGRaphSizeNo" + queryGraphSizeCount.toString + "_" + runTimeFileIndex
          val outputResultFilePath = outputResultFilePathOrigin  + "_dataGraphsubfix" + subfix.toString + "_varyingDataGRaphSizeNo" + queryGraphSizeCount.toString + "_" + runTimeFileIndex

          //general non-star query execution
          nonStarQuery.nonStarQueryExecute(sc, hierGraph, starQueryNodeLst, dstTypeLst, nonStarQueryTOPK, databaseType, inputNodeInfoFilePathTmp, outputResultFilePath, runTimeOutputFilePath, hierarchialRelation)     //execute non star query
          queryGraphSizeCount += 1
        }
      
     }
  }
  
  
  
}