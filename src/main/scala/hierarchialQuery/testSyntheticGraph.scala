/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.scala.hierarchialQuery

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object testSyntheticGraph {
  
  //test tiny graph to verify the score and bounding
  def testTinyGraphData(args: Array[String], sc: SparkContext, hierarchialRelation: Boolean) ={
    //val inputEdgeListfilePath = "../../Data/testInput/testEdgeListFile01"
    // val inputNodeInfoFilePath = "../../Data/testInput/testNodeInfo01"

    //val inputEdgeListfilePath = "../../Data/testInput/testEdgeListFile02"
    //val inputNodeInfoFilePath = "../../Data/testInput/testNodeInfo02"
    
    val inputEdgeListfilePath = "../../Data/testInput/testEdgeListFile04"
    val inputNodeInfoFilePath = "../../Data/testInput/testNodeInfo04"
    
    //read edge list to graphX graph
    val hierGraph = graphInputCommon.readEdgeListFile(sc, inputEdgeListfilePath, inputNodeInfoFilePath, "\t")

    val dstTypeId = 0                    //0 hierarchical node   or 1
    val topK = args(0).toInt
    starQuery.TOPK = topK
    
    val databaseType = 2              //synthetic graph database   2
    val runTimeFileIndex = args(1)
    
    val specificReadLst =  List((1L, 2), (2L, 2))               // List((648027L, 2), (636461L, 2))        
    // val specificReadLst =  List((1L, 2)) //, (2L, 2), (5L,2))               // List((648027L, 2), (636461L, 2))        
    //  val specificReadLst =  List((1L, 2), (2L, 2), (5L,2))               // List((648027L, 2), (636461L, 2))        


    val outputFilePath = "../output/testInput/starQueryOutput/starOutputFilePath" + runTimeFileIndex
    val runTimeoutputFilePath = "../output/testInput/starQueryOutput/starQueryoutRuntime" + runTimeFileIndex
    starQuery.starQueryExeute(sc, hierGraph, specificReadLst, dstTypeId, databaseType, inputNodeInfoFilePath,  outputFilePath, runTimeoutputFilePath, hierarchialRelation)     //execute star query
    
    
  }
  
    def executeSyntheticDatabase(args: Array[String], sc: SparkContext, hierarchialRelation: Boolean) = {
      
      val inputEdgeListfilePath = "../../Data/syntheticGraph/syntheticGraph_hierarchiRandom/syntheticGraphEdgeListInfo.tsv"
      val inputNodeInfoFilePath = "../../Data/syntheticGraph/syntheticGraph_hierarchiRandom/syntheticGraphNodeInfo.tsv"
        
      //read edge list to graphX graph
      val hierGraphRdd = graphInputCommon.readEdgeListFile(sc, inputEdgeListfilePath, inputNodeInfoFilePath, "\t")

       //executeStarQuerySyntheticDatabase(args, sc, hierGraphRdd, inputNodeInfoFilePath, hierarchialRelation)
        
       val inputGeneralQueryGraph = "../../Data/syntheticGraph/inputQueryGraph/generalQueryGraph/generateQuerygraphInput"
    
       executeGeneralQuerySyntheticDatabase(args, sc, hierGraphRdd, inputGeneralQueryGraph, inputNodeInfoFilePath: String, hierarchialRelation)
       
    }
  
  //../hierarchicalNetworkQuery/extractSubgraph/output/starQueryInput
  //start query synthetic database execution -- main entry
  def executeStarQuerySyntheticDatabase[VD, ED](args: Array[String], sc: SparkContext, dataGraph: Graph[VD, ED], inputNodeInfoFilePath: String, hierarchialRelation: Boolean) = {
   
    
    val dstTypeId = 0                    //0 hierarchical node   or 1
    val topK = args(0).toInt
    starQuery.TOPK = topK
    
    val databaseType = 2              //synthetic graph database   2
    val runTimeFileIndex = args(1)

    //val specificReadLst = List((648027L, 2), (636461L, 2))        
    
    //val specificReadLst = List((624793L, 2), (619226L, 2))            // three or more query graph size
    //val specificReadLst = List((695138L, 2), (655399L, 2))        // three or more query graph size
     
    //val specificReadLst = List((628210L, 2), (662132L, 2), (609465L, 2))        // three or more query graph size
    //val specificReadLst = List((662132L, 2), (609465L, 2))        // three or more query graph size

    //val specificReadLst = List((695138L, 2), (655399L, 2))        // three or more query graph size
    
    val specificReadLst = List((695138L, 2), (655399L, 2), (621354L, 2))        // three or more query graph size
    //val specificReadLst = List((698890L, 2), (631375L, 2), (664113L, 2))        // three or more query graph size
    

    val outputFilePath = "../output/syntheticData/starQueryOutput/starOutputFilePath" + runTimeFileIndex
    val runTimeoutputFilePath = "../output/syntheticData/starQueryOutput/starQueryoutRuntime" + runTimeFileIndex
    starQuery.starQueryExeute(sc, dataGraph, specificReadLst, dstTypeId, databaseType, inputNodeInfoFilePath,  outputFilePath, runTimeoutputFilePath, hierarchialRelation)     //execute star query
    
       
  }

  
   // general general query entry (non-star query) for synthetic graph
  def executeGeneralQuerySyntheticDatabase[VD, ED](args: Array[String], sc: SparkContext, dataGraph: Graph[VD, ED], inputGeneralQueryGraph: String, inputNodeInfoFilePath: String, hierarchialRelation: Boolean) = {
 
    val allquerySizeLsts = inputQueryRead.getDecomposedStarQuerySpecificNodes(sc, inputGeneralQueryGraph)
    val topK = args(0).toInt
    starQuery.TOPK = topK
    val databaseType = 2              //synthetic graph database   2
    
    print ("main allquerySizeLsts： " + allquerySizeLsts + "\n")
    //for varing query graph size
    var runTimeOutputFilePath = ""
    var outputResultFilePath = ""
    if (hierarchialRelation){
        runTimeOutputFilePath = "../output/syntheticData/nonStarQueryOutput/testWithHierarchiQueryOutput/" + "queryRuntime"
        outputResultFilePath = "../output/syntheticData/starQueryOutput/nonStarQueryOutput/testWithHierarchiQueryOutput/" + "runResult"
    }
    else{
        runTimeOutputFilePath = "../output/syntheticData/nonStarQueryOutput/testWOHierarchiQueryOutput/" + "queryRuntime"
        outputResultFilePath = "../output/syntheticData/starQueryOutput/nonStarQueryOutput/testWOHierarchiQueryOutput/" + "runResult"
    }
    
    var count = 1

    for (specNodelistStarQueryLst <- allquerySizeLsts)
    {
       //print ("executeGeneralQuerySyntheticDatabase specNodelistStarQueryLst： " + specNodelistStarQueryLst + "\n")
       val nonStarQueryTOPK = starQuery.TOPK
       val starQueryNodeLst = specNodelistStarQueryLst._1
       val dstTypeLst = specNodelistStarQueryLst._2

      print ("starQueryNodeLst： " + starQueryNodeLst + " " + dstTypeLst+  "\n")
      //general query 
      runTimeOutputFilePath = runTimeOutputFilePath + i.toString + "_top" + nonStarQueryTOPK.toString + "_times" + runTimeFileIndex
      val nonStarQueryTOPK = starQuery.TOPK
      outputResultFilePath = outputResultFilePath + i.toString + "_top" + nonStarQueryTOPK.toString + "_times" + runTimeFileIndex
      
      //general non-star query execution
      nonStarQuery.nonStarQueryExecute(sc, dataGraph, starQueryNodeLst, dstTypeLst, nonStarQueryTOPK, databaseType, inputNodeInfoFilePath, outputResultFilePath, tmpRunTimeoutputFilePath, hierarchialRelation)     //execute non star query
  
    }
    
    
    
  }
  
}
