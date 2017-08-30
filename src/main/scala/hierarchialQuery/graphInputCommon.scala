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

//common function for the project
object graphInputCommon {
  
  //http://biercoff.com/easily-measuring-code-execution-time-in-scala/
  def runTimeforFunction[R](block: => R): R = {  
    val t0 = System.currentTimeMillis()       //System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()       //System.nanoTime()
    //println("Elapsed time: " + (t1 - t0) + "ms")
    result
  }
  //
  
  //read hierarchical edge list (src_node_id  dst_node_id edge_property);   delimiter '\t'
  def readEdgeListFile(sc: SparkContext, inputEdgeFilePath: String, inputNodeInfoFilePath: String, delimiter: String) ={
    
    //read edge list file
    val fileEdgeList = sc.textFile(inputEdgeFilePath);
    // create edge RDD of type RDD[(VertexId, VertexId)]             
    val origEdgeRdd = fileEdgeList.map(line => line.split(delimiter))
      .map(line => (line(0), line(1), line(2)))

    //edgesRDD:   edge property: int   (hierarchical level)
    val edgesRDD: RDD[Edge[Int]] = origEdgeRdd.map{
      case (a, b, edge) =>
        
          //transfer hierarchical level to int
          def funcTransferEdgeToInt(edge: String) = {
            if (edge == "same"){
              0
            }
            else if (edge == "higher"){
              1
            }
            else{
              -1
            }
        }
        Edge(a.toLong, b.toLong, funcTransferEdgeToInt(edge))

    }

    //read nodeInfo 
    val fileNodeInfo = sc.textFile(inputNodeInfoFilePath)    
    
    // create edge RDD of type RDD[(VertexId, VertexId)]             
    val origNodeRdd = fileNodeInfo.map(line => line.split(delimiter))
      .map(line => (line(0), line(1)))
      
    
    //read nodeInfo file to get vertexRdd    verticesRDD: (nodeId, nodeIdType)

    val verticesRDD :RDD[(VertexId, Int)] = origNodeRdd.map{
      case (nodeNameType, nodeId) => 
        
      def funcGetNodeType(nodeNameType: String) = {
        val nodeTypeId = nodeNameType.replace("\"", "").split("\\+\\+\\+")(1).trim.toInt       //return node Type Id; use "\\+", not "+++" regular expression
         //println("nodeTypeId : " + nodeTypeId)
        
          nodeTypeId
      }
      (nodeId.toLong, funcGetNodeType(nodeNameType))
    }
    
    verticesRDD.take(115).foreach(println)
    edgesRDD.take(115).foreach(println)
     // val vertMapRdd = vertMapRdd1.union(vertMapRdd2).distinct()
    //val verticesRDD :RDD[(VertexId, Int)] = vertMapRdd.map{
    //  case(id, nodeIdType) => (id.toLong, nodeIdType)                    
   // }
    
     // create a graph 
    val hierGraph = Graph(verticesRDD, edgesRDD)
    
    //vertMapRdd.take(5).foreach(println)
    //you can see your graph 
    //print ("count:" +hierGraph.vertices.count)
    //print ("count checkpoint: ",  hierGraph.vertices.checkpoint())
    println("node vertices edgecount: ", hierGraph.vertices.count(), hierGraph.edges.count())

   // hierGraph.vertices.take(1).foreach(println)
    //hierGraph.edges.take(5).foreach(println)
    //hierGraph.triplets.take(5).foreach(println)
    hierGraph
  }
  
  //read the node info into RDD;  read node info into RDD structure
  def readNodeInfoName(sc: SparkContext, inputFileNodeInfoPath: String)= {
      val file = sc.textFile(inputFileNodeInfoPath)
      val nodeInfoRdd :RDD[(VertexId, (String, String, String))] =  file.map(line => line.split("\t"))
      .map(line => (line(0).toLong, (line(1), line(2), line(3))))         //nodeId, nodename, nodeTypeId, nodeTypeName
      
      //nodeInfoRdd.take(5).foreach(println)
      nodeInfoRdd
    
  }
  
  
  //transfer adjacency neighbor  of each line to a list
  def transferAdjacencyNeighborToList(adj: String) = {
       
    var lst = List[(String, Int)]()

    if (adj contains "higher")
    {
      val beginInd = adj.indexOfSlice("higher")
      val arrayLst = adj.slice(beginInd+8,  adj.length).split(" ")
      var i = 0  
      var done = false
      //println ("type arraylst: "+ type(arrayLst))
      while (i < arrayLst.length && !done)
      {
          if ((arrayLst(i) contains "same") || (arrayLst(i) contains "lower"))
          {
            done = true
          }
          if (!done)
          {
              lst = (arrayLst(i), 1) :: lst             // higher distance is 1.
          }
          i += 1
      }
    }

    if(adj contains "lower")
    {
      val beginInd = adj.indexOfSlice("lower")
      val arrayLst = adj.slice(beginInd+7,  adj.length).split(" ")
      var i = 0
      var done = false
      //println ("type arraylst: "+ type(arrayLst))
      while (i < arrayLst.length && !done)
      {
          if ((arrayLst(i) contains "higher") || (arrayLst(i) contains "same"))
          {
            done = true
          }
          if (!done)
          {
              lst = (arrayLst(i), -1) :: lst             // lower hierarchical  distance is 1.
          }
          i += 1
      }

    }

    if(adj contains "same")
    {
      val beginInd = adj.indexOfSlice("same")
      val arrayLst = adj.slice(beginInd+6,  adj.length).split(" ")
      var i = 0
      var done = false
      //println ("type arraylst: "+ type(arrayLst))
      while (i < arrayLst.length && !done)
      {
          if ((arrayLst(i) contains "higher") || (arrayLst(i) contains "lower"))
          {
            done = true
          }
          if (!done)
          {
              lst = (arrayLst(i), 0) :: lst             // same hierarchical distance is 0.
          }
          i += 1
      }
    }

    lst
  }
  
  //read adjcency list file into RDD and then construct to graph
  def readAdjcencyListFile(sc: SparkContext, inputAdjacencyListfilePath: String) = {
    
    val file = sc.textFile(inputAdjacencyListfilePath)

    // create edge RDD of type RDD[(VertexId, VertexId)]
    val origRdd = file.map(line => line.split("\t"))
        .map(line => (line(0).toLong, line(1).toInt, line(2)))
    
    //graph VD is int, which is node type id
    val vRDD: RDD[(VertexId, Int)] = origRdd.map{
      case (nd, typeId, adj) => (nd, typeId)                      //(nodeId, nodeIdType)
    }
    
   //val nodeInfoRdd = readNodeInfoName(sc, inputFileNodeInfoPath)       //nodeId, nodeName, nodeTypeName
   // val vRDD: RDD[(VertexId, Int)]= vertMapRddTmp.join(nodeInfoRdd).map{
   //   case (nodeId, nodeAttr) => (nodeId.toLong, nodeAttr._1.toInt)                  //nodeId, nodeTypeId        //tuple not accessed by nodeAttr(0) but nodeAttr._1       
   // }                            
    
    //vertMapRdd.collect.foreach(println)
   // vRDD.take(5).foreach(println)

    val tempEdgeRdd = origRdd.map{
      case (nd, typeId, adj) =>
        
        val lst = transferAdjacencyNeighborToList(adj)
        (nd, lst)
    }
   
    //tempEdgeRdd.collect.foreach(println) 
     //graph ED is int, hierarcal level distance
     val eRDD: RDD[Edge[Int]] = tempEdgeRdd.flatMap(x => x._2.map(y => (x._1, y))).map{
      case(id1, id2edge) =>  

        if (id2edge._1.toString  == "")                       //may exist some isolated node
        { 
            Edge(id1.toLong, -10001L, 0)
        }
        else
        {
          Edge(id1.toLong, id2edge._1.toLong, id2edge._2)
        }
      }          //nodeId, nodeId, edgerelation(hierarchical level, Longtype)
    
    //eRDD.take(10).foreach(println)
    //eRDD.collect().foreach(println)

    //test
    //val testout = "/home/fubao/workDir/ResearchProjects/hierarchicalNetworkQuery/inputData/ciscoProductVulnerability/testout"
    //eRDD2.saveAsTextFile(testout)
    
    val hierGraph = Graph(vRDD, eRDD)              //graph[VD,ED], VD is int,indicating node type id;  ED is int, indicating hierarchical level info
    //println("node vertices edgecount: ", hierGraph.vertices.count, hierGraph.edges.count)

    //hierGraph.edges.take(5).foreach(println)

    hierGraph
    
  }
   
  
  def test(sc: SparkContext) = {
      val users: RDD[(VertexId, Int)] =
    sc.parallelize(Array((3L, 1), (7L, 3),
                         (5L, 2), (2L, 3)))
    // Create an RDD for edges
    val relation: RDD[Edge[Int]] =
      sc.parallelize(Array(Edge(3L, 7L, 1),    Edge(5L, 3L, -1),
                           Edge(2L, 5L, 0), Edge(5L, 7L, 1)))
    
    val relationships: RDD[Edge[Int]] = relation.map(x => Edge(x.srcId, x.dstId, x.attr))
        users.foreach(println)
        relation.foreach(println)
        relationships.foreach(println)
   // println ("type vRDD, eRDD : ", type(users), type(relation), type(relationships))

    // Define a default user in case there are relationship with missing user
    //val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships)
    val edgesMapRddTest = graph.edges
    //edgesMapRddTest.foreach(println)
    
    graph
  }
    
}
