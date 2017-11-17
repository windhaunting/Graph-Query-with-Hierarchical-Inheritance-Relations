/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.scala.hierarchialQuery

import org.apache.spark.SparkContext

object testSyntheticGraph {
  
  //test tiny graph to verify the score and bounding
  def testTinyGraphData(args: Array[String], sc: SparkContext) ={
    //val inputEdgeListfilePath = "../../Data/testInput/testEdgeListFile01"
    // val inputNodeInfoFilePath = "../../Data/testInput/testNodeInfo01"

    val inputEdgeListfilePath = "../../Data/testInput/testEdgeListFile02"
    val inputNodeInfoFilePath = "../../Data/testInput/testNodeInfo02"
    
    //read edge list to graphX graph
    val hierGraph = graphInputCommon.readEdgeListFile(sc, inputEdgeListfilePath, inputNodeInfoFilePath, "\t")

    val dstTypeId = 0                    //0 hierarchical node   or 1
    val topK = args(0).toInt
    starQuery.TOPK = topK
    
    val databaseType = 2              //synthetic graph database   2
    val runTimeFileIndex = args(1)
    
     val specificReadLst =  List((1L, 2), (2L, 2))               // List((648027L, 2), (636461L, 2))        

     //val specificReadLst =  List((1L, 2)) //, (2L, 2), (5L,2))               // List((648027L, 2), (636461L, 2))        

    val hierarchialRelation = true

    val outputFilePath = "../output/testInput/starQueryOutput/starOutputFilePath" + runTimeFileIndex
    val runTimeoutputFilePath = "../output/testInput/starQueryOutput/starQueryoutRuntime" + runTimeFileIndex
    starQuery.starQueryExeute(sc, hierGraph, specificReadLst, dstTypeId, databaseType, inputNodeInfoFilePath,  outputFilePath, runTimeoutputFilePath, hierarchialRelation)     //execute star query
    
    
  }
  
  
  
  //../hierarchicalNetworkQuery/extractSubgraph/output/starQueryInput
  //product database execution -- main entry
  def executeSyntheticDatabase(args: Array[String], sc: SparkContext) = {
   
    
     val inputEdgeListfilePath = "../../Data/syntheticGraph/syntheticGraph_hierarchiRandom/syntheticGraphEdgeListInfo.tsv"
     val inputNodeInfoFilePath = "../../Data/syntheticGraph/syntheticGraph_hierarchiRandom/syntheticGraphNodeInfo.tsv"
        
    //read edge list to graphX graph
    val hierGraph = graphInputCommon.readEdgeListFile(sc, inputEdgeListfilePath, inputNodeInfoFilePath, "\t")

    val dstTypeId = 0                    //0 hierarchical node   or 1
    val topK = args(0).toInt
    starQuery.TOPK = topK
    
    val databaseType = 2              //synthetic graph database   2
    val runTimeFileIndex = args(1)

    val specificReadLst = List((648027L, 2), (636461L, 2))        
    
    val hierarchialRelation = true

    val outputFilePath = "../output/syntheticData/starQueryOutput/starOutputFilePath" + runTimeFileIndex
    val runTimeoutputFilePath = "../output/syntheticData/starQueryOutput/starQueryoutRuntime" + runTimeFileIndex
    starQuery.starQueryExeute(sc, hierGraph, specificReadLst, dstTypeId, databaseType, inputNodeInfoFilePath,  outputFilePath, runTimeoutputFilePath, hierarchialRelation)     //execute star query
    
       
  }


}
