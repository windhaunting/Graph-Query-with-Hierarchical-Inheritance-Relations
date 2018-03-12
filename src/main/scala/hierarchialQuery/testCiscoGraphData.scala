/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.scala.hierarchialQuery

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object testCiscoGraphData {
  
  //product database execution -- main entry
  def executeProductDatabase(args: Array[String], sc: SparkContext, hierarchialRelation: Boolean) = {
    
    
    val inputEdgeListfilePath = "../../Data/ciscoDataGraph/ciscoDataGraphInfo1.0/edgeListPart1.0"
    val inputNodeInfoFilePath = "../../Data/ciscoDataGraph/ciscoDataGraphInfo1.0/nodeInfoPart1.0"
    
    //read edge list to graphX graph
    val hierGraphRdd = graphInputCommon.readEdgeListFile(sc, inputEdgeListfilePath, inputNodeInfoFilePath, "\t")

    //starQueryCiscoData(args, sc， hierGraphRdd, inputNodeInfoFilePath, hierarchialRelation)
    
    val inputGeneralQueryGraph = "../../Data/ciscoDataGraph/inputQueryGraph/generalQueryGraph/generateQuerygraphInput"

    executeGeneralQueryCiscoDatabase(args, sc, hierGraphRdd, inputGeneralQueryGraph, inputNodeInfoFilePath, hierarchialRelation)

    
   // executeGeneralQueryCiscoDatabaseDifferentTopK(args, sc, hierGraphRdd, inputGeneralQueryGraph, inputNodeInfoFilePath, hierarchialRelation)
    
   // executeGeneralQueryCiscoDatabaseDifferentQuerySize(args, sc, hierGraphRdd, inputGeneralQueryGraph, inputNodeInfoFilePath, hierarchialRelation)
    
    
   
     /*
     // test different data graph
     val dataGraphPathPrefix = "../../../hierarchicalNetworkQuery/extractSubgraph/output/ciscoDataGraphExtractOut/dataGraphInfo"
     val inputGeneralQueryGraphPrefix = "../../../hierarchicalNetworkQuery/extractSubgraph/output/ciscoDataGraphExtractOut/inputGeneralQueryGraph/queryGraphInput"
     
     executeGeneralQueryCiscoDatabaseDifferentDataSize(args, sc, dataGraphPathPrefix, inputGeneralQueryGraphPrefix, hierarchialRelation)
    */
    
    
  }
  
    //entry for star query for cisco data
   def starQueryCiscoData[VD, ED](args: Array[String], sc: SparkContext, dataGraph: Graph[VD, ED], inputNodeInfoFilePath: String, hierarchialRelation: Boolean) = {
    
    // val file = "hdfs://localhost:8070/testEdgeListFile2")
    //val file = "hdfs://192.168.0.52:8070/testEdgeListFile2"

    val dstTypeId = 0                    //0 hierarchical node   or 1
    val topK = args(0).toInt
    starQuery.TOPK = topK
    
    val databaseType = 0              //Cisco data graph database   0
    val runTimeFileIndex = args(1)
    
    //val specificReadLst = List((3237L, 1), (5446L, 1))        // three or more query graph size
    //val specificReadLst = List((2020L, 1), (9021L, 4))        // three or more query graph size
    //val specificReadLst = List((3237L, 1), (5446L, 1), (3243L, 1))        // three or more query graph size

    val specificReadLst = List((2020L, 1), (9021L, 4), (9024L, 4))        // three or more query graph size

    val outputFilePath = "../output/ciscoProduct/starQueryOutput/starQueryOutputFilePath" + runTimeFileIndex
    val runTimeoutputFilePath = "../output/ciscoProduct/starQueryOutput/starQueryoutRuntime" + runTimeFileIndex
    starQuery.starQueryExeute(sc, dataGraph, specificReadLst, dstTypeId, databaseType, inputNodeInfoFilePath,  outputFilePath, runTimeoutputFilePath, hierarchialRelation)     //execute star query
    
 
   }
   
  
  // general general query entry (non-star query) for cisco data graph
  def executeGeneralQueryCiscoDatabase[VD, ED](args: Array[String], sc: SparkContext, dataGraph: Graph[VD, ED], inputGeneralQueryGraph: String, inputNodeInfoFilePath: String, hierarchialRelation: Boolean) = {
    
    val allquerySizeLsts = inputQueryRead.getDecomposedStarQuerySpecificNodes(sc, inputGeneralQueryGraph)
    
    val topK = args(0).toInt      //topK
    starQuery.TOPK = topK
    val databaseType = 0             //cisco graph database   2
    
    val runTimeFileIndex = args(1)
    
    print ("main allquerySizeLsts： " + allquerySizeLsts + "\n")
    //for varing query graph size
    var runTimeOutputFilePath = ""
    var outputResultFilePath = ""
    if (hierarchialRelation){
        runTimeOutputFilePath = "../output/ciscoProduct/nonStarQueryOutput/testWithHierarchiQueryOutput/" + "queryRuntime"
        outputResultFilePath = "../output/ciscoProduct/nonStarQueryOutput/testWithHierarchiQueryOutput/" + "runResult"
    }
    else{
        runTimeOutputFilePath = "../output/ciscoProduct/nonStarQueryOutput/testWOHierarchiQueryOutput/" + "queryRuntime"
        outputResultFilePath = "../output/ciscoProduct/nonStarQueryOutput/testWOHierarchiQueryOutput/" + "runResult"
    }
    
    var count = 1

    for (specNodelistStarQueryLst <- allquerySizeLsts)
    {
       //print ("executeGeneralQuerySyntheticDatabase specNodelistStarQueryLst： " + specNodelistStarQueryLst + "\n")
       val starQueryNodeLst = specNodelistStarQueryLst._1
       val dstTypeLst = specNodelistStarQueryLst._2

      print ("executeGeneralQueryCiscoDatabase starQueryNodeLst： " + starQueryNodeLst + " " + dstTypeLst+  "\n")
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
  def executeGeneralQueryCiscoDatabaseDifferentTopK[VD, ED](args: Array[String], sc: SparkContext, dataGraph: Graph[VD, ED], inputGeneralQueryGraph: String, inputNodeInfoFilePath: String, hierarchialRelation: Boolean) = {
 
    val allquerySizeLsts = inputQueryRead.getDecomposedStarQuerySpecificNodes(sc, inputGeneralQueryGraph)
    //val topK = args(0).toInt      //topK
    //starQuery.TOPK = topK
    val databaseType = 0              //synthetic graph database   2
    
    val runTimeFileIndex = args(0)           
    
    print ("main allquerySizeLsts： " + allquerySizeLsts + "\n")
    //for varing query graph size
    var runTimeOutputFilePathOrigin = ""
    var outputResultFilePathOrigin = ""
    if (hierarchialRelation){
        runTimeOutputFilePathOrigin = "../output/ciscoProduct/nonStarQueryOutput/testWithHierarchiQueryOutput/" + "queryRuntime"
        outputResultFilePathOrigin = "../output/ciscoProduct/nonStarQueryOutput/testWithHierarchiQueryOutput/" + "runResult"
    }
    else{
        runTimeOutputFilePathOrigin = "../output/ciscoProduct/nonStarQueryOutput/testWOHierarchiQueryOutput/" + "queryRuntime"
        outputResultFilePathOrigin = "../output/ciscoProduct/nonStarQueryOutput/testWOHierarchiQueryOutput/" + "runResult"
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
  def executeGeneralQueryCiscoDatabaseDifferentQuerySize[VD, ED](args: Array[String], sc: SparkContext, dataGraph: Graph[VD, ED], inputGeneralQueryGraph: String, inputNodeInfoFilePath: String, hierarchialRelation: Boolean) = {
 
    val allquerySizeLsts = inputQueryRead.getDecomposedStarQuerySpecificNodes(sc, inputGeneralQueryGraph)
    //val topK = args(0).toInt      //topK
    //starQuery.TOPK = topK
    val databaseType = 0              //synthetic graph database   2
    
    val runTimeFileIndex = args(0)           
    
    print ("main allquerySizeLsts： " + allquerySizeLsts.size + "\n")
    //for varing query graph size
    var runTimeOutputFilePathOrigin = ""
    var outputResultFilePathOrigin = ""
    if (hierarchialRelation){
        runTimeOutputFilePathOrigin = "../output/ciscoProduct/nonStarQueryOutput/testWithHierarchiQueryOutput/varyingQueryGraphSizeOneMachine/" + "queryRuntime"
        outputResultFilePathOrigin = "../output/ciscoProduct/nonStarQueryOutput/testWithHierarchiQueryOutput/varyingQueryGraphSizeOneMachine/" + "runResult"
    }
    else{
        runTimeOutputFilePathOrigin = "../output/ciscoProduct/nonStarQueryOutput/testWOHierarchiQueryOutput/" + "queryRuntime"
        outputResultFilePathOrigin = "../output/ciscoProduct/nonStarQueryOutput/testWOHierarchiQueryOutput/" + "runResult"
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
  
  
  
  // varing different data graph 10%, 20%, 50%, 80%, 100%;  genera query entry (non-star query) for cisco graph
  def executeGeneralQueryCiscoDatabaseDifferentDataSize[VD, ED](args: Array[String], sc: SparkContext, inputDataGraphPathPrefix: String, inputGeneralQueryGraphPrefix: String, hierarchialRelation: Boolean) = {
 
     //val topK = args(0).toInt      //topK
     //starQuery.TOPK = topK
     val databaseType = 0              //synthetic graph database   2
    
     val runTimeFileIndex = args(0)           
    
    //for varing query graph size
    var runTimeOutputFilePathOrigin = ""
    var outputResultFilePathOrigin = ""
    if (hierarchialRelation){
        runTimeOutputFilePathOrigin = "../output/ciscoProduct/nonStarQueryOutput/testWithHierarchiQueryOutput/varyingDataGraphSizeOneMachine/" + "queryRuntime"
        outputResultFilePathOrigin = "../output/ciscoProduct/nonStarQueryOutput/testWithHierarchiQueryOutput/varyingDataGraphSizeOneMachine/" + "runResult"
    }
    else{
        runTimeOutputFilePathOrigin = "../output/ciscoProduct/nonStarQueryOutput/testWOHierarchiQueryOutput/varyingDataGraphSizeOneMachine/" + "queryRuntime"
        outputResultFilePathOrigin = "../output/ciscoProduct/nonStarQueryOutput/testWOHierarchiQueryOutput/varyingDataGraphSizeOneMachine/" + "runResult"
    }
    
     starQuery.TOPK = 5
     val nonStarQueryTOPK = starQuery.TOPK
    
     val subfixs = List("0.1", "0.2", "0.5", "0.8", "1.0")   // List("0.1", "0.2", "0.5", "0.8", "1.0") //List("0.1")  //
     
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