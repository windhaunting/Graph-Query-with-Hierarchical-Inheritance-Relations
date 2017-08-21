/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.scala.hierarchialQuery

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators
import scala.util.MurmurHash

import org.wish.spark.graphx.graphInputCommon

class QueryMain {
  def main(args: Array[String]) {
    
    val appIdName = "Graph query with hierarhcial relation"
    val conf = new SparkConf().setAppName(appIdName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer","24")               // Now it's 24 Mb of buffer by default instead of 0.064 Mb

    val sc = new SparkContext(conf)
    
   // val file = "hdfs://localhost:8070/testEdgeListFile2")
   //val file = "hdfs://192.168.0.52:8070/testEdgeListFile2"
    //var file = "/home/fubao/workDir/CiscoWish/QueryGraph/mazerunner/sparkMaven/testEdgeListFile2"

    // inputDataToClusterOnePair(sc, args)
    //createGraph(sc)
    val graphInputCommonObj = graphInputCommon()
    //readFile(sc)
  }
}
