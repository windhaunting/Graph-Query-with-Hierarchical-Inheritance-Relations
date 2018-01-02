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

    //starQueryCiscoData(args, sc， hierGraph, inputNodeInfoFilePath, hierarchialRelation)
    
    val inputGeneralQueryGraph = "../../Data/ciscoDataGraph/inputQueryGraph/generalQueryGraph/generateQuerygraphInput"

    executeGeneralQueryCiscoDatabase(args, sc, hierGraphRdd, inputGeneralQueryGraph, inputNodeInfoFilePath: String, hierarchialRelation)

    
  }
  
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
   
  
  // general general query entry (non-star query) for synthetic graph
  def executeGeneralQueryCiscoDatabase[VD, ED](args: Array[String], sc: SparkContext, dataGraph: Graph[VD, ED], inputGeneralQueryGraph: String, inputNodeInfoFilePath: String, hierarchialRelation: Boolean) = {
    val allquerySizeLsts = inputQueryRead.getDecomposedStarQuerySpecificNodes(sc, inputGeneralQueryGraph)
    
    val topK = args(0).toInt      //topK
    starQuery.TOPK = topK
    val databaseType = 2              //synthetic graph database   2
    
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
  
}

