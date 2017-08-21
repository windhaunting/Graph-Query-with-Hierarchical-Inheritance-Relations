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
import scala.util.MurmurHash

class graphInputCommon {
  
def readAdjcencyListFile(sc: SparkContext, inputFilePath: String) ={
    
      
    val file = sc.textFile("graphXInput/testFile.csv");

    // create edge RDD of type RDD[(VertexId, VertexId)]
    val origRdd = file.map(line => line.split(" "))
      .map(line => (line(0), line(1), line(2)))
        

    val edgesRDD: RDD[Edge[Double]] = origRdd.map{
      case (a, b, w) =>
        Edge(MurmurHash.stringHash(a.toString), MurmurHash.stringHash(b.toString), w.toDouble)}

    
    val vertMapRdd1 = origRdd.map{
      case (a, b, w) =>
        (MurmurHash.stringHash(a.toString), a)
    }
    
    val vertMapRdd2 = origRdd.map{
      case (a, b, w) =>
        (MurmurHash.stringHash(b.toString), b)
    }
    
    val vertMapRdd = vertMapRdd1.union(vertMapRdd2).distinct()
    // create a graph 
 //   val graph = Graph.fromEdges(edgesRDD, "defaultProperty")

    val verticesRDD :RDD[(VertexId, String)] = vertMapRdd.map{
      case(id, att) => (id.toLong, att)
    }
    
    val graph = Graph.apply(verticesRDD, edgesRDD)
    
    vertMapRdd.collect.foreach(println)
    
    println("sssp.vertices.collect.mkString  ")

    // you can see your graph 
    graph.vertices.collect.foreach(println)
    graph.edges.collect.foreach(println)
    graph.triplets.collect.foreach(println)
  }
  
}
