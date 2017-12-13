/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.scala.hierarchialQuery

import org.apache.spark.SparkContext


object testDblpGraphData {
  
  
//dblp data base execute --main entry
  def executeDblpGraphData(args: Array[String], sc: SparkContext) = {
      
    starQueryDblpData(args, sc)
    
    
    //val inputEdgeListfilePath = "../../Data/dblpParserGraph/output/finalOutput/newOutEdgeListFile.tsv"
    //val inputNodeInfoFilePath = "../../Data/dblpParserGraph/output/finalOutput/newOutNodeNameToIdFile.tsv"
        
    //read edge list to graphX graph
    //val hierGraph = graphInputCommon.readEdgeListFile(sc, inputEdgeListfilePath, inputNodeInfoFilePath, "\t")

    // val dstTypeId = 1                     //1: people
   // val topK = args(0).toInt
   // starQuery.TOPK = topK
    
   // val databaseType = 1              //DBLP database
   // val runTimeFileIndex = args(1)

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
    val runTimeOutputFilePath = "../output/dblpData/starQueryOutput/varingTopKOneMachine/starQueryoutRuntime" + runTimeFileIndex
    starQuery.starQueryExeute(sc, hierGraph, specificReadLst, dstTypeId, databaseType, inputNodeInfoFilePath,  null, runTimeOutputFilePath)     //execute star query
    */
   
    //val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((189059L, 3), (10821L,1)), List((189059L, 3), (189086L, 3)))
    //val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((189059L, 3), (189086L, 3)))

    /* 
    val specNodelistStarQueryTwoDimension: List[List[(VertexId, Int)]] = List(List((189059L, 3), (10821L,1)), List((189059L, 3), (189086L, 3)), List((188857L, 3), (189086L, 3)))
    val runTimeOutputFilePath = "../output/dblpData/nonStarQueryOutput/varingTopKOneMachine/nonStarQueryoutRuntime" + runTimeFileIndex
    val outputFilePath = null      //"../output/dblpData/nonStarQueryOutput/varingTopKOneMachine/nonStarQueryOutputFilePath" + runTimeFileIndex

    var dstTypeIdLstBuffer: ListBuffer[Int] = new ListBuffer[(Int)]
    for (specNodeLst <- specNodelistStarQueryTwoDimension)
    {
         
        dstTypeIdLstBuffer += (1)
    }
    
    val nonStarQueryTOPK = starQuery.TOPK
    nonStarQuery.nonStarQueryExecute(sc, hierGraph, specNodelistStarQueryTwoDimension, dstTypeIdLstBuffer, nonStarQueryTOPK, databaseType, inputNodeInfoFilePath, outputFilePath, runTimeoutputFilePath)     //execute star query
    
    */
  
    /*
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
      nonStarQuery.nonStarQueryExecute(sc, hierGraph, specNodelistStarQueryTwoDimension, dstTypeIdLstBuffer, 
                                   nonStarQueryTOPK, databaseType, inputNodeInfoFilePath, null, tmpRunTimeOutputFilePath)     //execute non star query
    
    }
   */
  
    //test varing graphData in dblp data
    //val graphSizeRatio = args(2).toInt
    //val hierarchialRelation = true
    //testVaringGraphDataDblp( sc, topK, runTimeFileIndex,  graphSizeRatio, databaseType, hierarchialRelation)
    
   // val hierarchialRelation = false
   // testHierarchicalRelationDblpData (sc, topK, runTimeFileIndex, databaseType, hierarchialRelation)
    
  }
  

  def starQueryDblpData(args: Array[String], sc: SparkContext) = {
      
    val inputEdgeListfilePath = "../../Data/dblpParserGraph/output/finalOutput/newOutEdgeListFile.tsv"
    val inputNodeInfoFilePath = "../../Data/dblpParserGraph/output/finalOutput/newOutNodeNameToIdFile.tsv"
        
    //read edge list to graphX graph
    val hierGraph = graphInputCommon.readEdgeListFile(sc, inputEdgeListfilePath, inputNodeInfoFilePath, "\t")

    val dstTypeId = 1                    //0 hierarchical node   or 1
    val topK = args(0).toInt
    starQuery.TOPK = topK
    
    val databaseType = 1              //dblp data graph database   1
    val runTimeFileIndex = args(1)
    
    val specificReadLst = List((188421L, 3), (188806L, 3))        // three or more query graph size
     
    val hierarchialRelation = true
   
    val outputFilePath = "../output/dblpData/starQueryOutput/starQueryOutputFilePath" + runTimeFileIndex
    val runTimeoutputFilePath = "../output/dblpData/starQueryOutput/starQueryoutRuntime" + runTimeFileIndex
    starQuery.starQueryExeute(sc, hierGraph, specificReadLst, dstTypeId, databaseType, inputNodeInfoFilePath,  outputFilePath, runTimeoutputFilePath, hierarchialRelation)     //execute star query
    
  
}

  
}