/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.scala.hierarchialQuery

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import scala.collection.mutable.ListBuffer
import scala.util.Random.shuffle


//get different query graph automatically here
object inputQueryRead {

  
  //get query size;  different query node number 2, 5, 10, 20, 50, 100
  //specific node type could be 1, 4, 5, 6 and query node type is 0
  def getQuerySpecifiNodesLst[VD, ED](sc: SparkContext, inputFileNodeInfoPath: String, specificNodeNumber: Int) = {  //: Seq[(VertexId, Int)]
     val file = sc.textFile(inputFileNodeInfoPath)
      val nodeInfoRdd :RDD[(VertexId, Int)] =  file.map(line => line.split("\t"))
      .map(line => (line(0).toLong, line(2).toInt))         //nodeId, nodename, nodeTypeId, nodeTypeName
      
     
     val specifiTyeLst = List(1, 4, 5, 6)
     val newSpecificNodesRdd = nodeInfoRdd.filter{
        case (nodeId, nodeTypeId) => specifiTyeLst.contains(nodeTypeId)
     }
     
     //nodeInfoRdd.take(15).foreach(println)
     //newSpecificNodesRdd.take(15).foreach(println)
     val newSpecificNodesListBuffer= newSpecificNodesRdd.collect().toList.to[ListBuffer]
     
     println("newSpecificNodesArray: ", newSpecificNodesListBuffer.length, newSpecificNodesListBuffer(1), newSpecificNodesListBuffer(2))

     //val newSpecificNodesListBufferShuffle = shuffle(newSpecificNodesListBuffer)        //shuffle the input specific node list
     var specificInputLst: List[(VertexId, Int)]  =  Nil
     var j = 0
     var i = 0
     while ( i < specificNodeNumber)
      {
         j = j + 10
       specificInputLst = newSpecificNodesListBuffer(j)::specificInputLst
       
       i = i + 1
      }
  
      specificInputLst 
     
  }
  
  //get query size; randomly extract subgraph from the data graph; and then delete some edge?
  //implemented by python networkX
  //here we read the output from the file generated by python networkx
  def getQuerySizeNumber[VD, ED](sc: SparkContext, inputFileSpecificStarQueryPath: String) = {
    
      val specNodelistStarQueryTwoDimension:List[List[(VertexId, Int)]] = Nil

      val file = sc.textFile(inputFileSpecificStarQueryPath)
      val specNodelistStarQueryRdd :RDD[List[List[(VertexId, Int)]]] =  file.map(line => line.split("\t")).map{
          line =>                      //line type: List(String)
          def transferToList() = {
             var starQueryNodeLst:List[List[(VertexId, Int)]] = Nil
             for (str <- line)
             {
                 //find ";"
                 var innerLst: List[(VertexId, Int)] = Nil
                 val substr = str.split(";")
                 for (innerSpec<- substr)
                   {
                     innerLst =  (innerSpec.split(",")(0).toLong, innerSpec.split(",")(1).toInt)::innerLst
                   }
                 starQueryNodeLst = innerLst::starQueryNodeLst
             }
             starQueryNodeLst
          }
          
        transferToList()
            
      }
      //specNodelistStarQueryRdd.take(10).foreach(println)
      specNodelistStarQueryRdd.collect().toList
      
     } 
    
}
