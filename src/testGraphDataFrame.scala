/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.scala.hierarchialQuery
import org.apache.spark.SparkContext

import org.graphframes._

object testGraphDataFrame {
  
  def test01(sc: SparkContext) ={

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val v = sqlContext.createDataFrame(List(
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 30),
    ("d", "David", 29),
    ("e", "Esther", 32),
    ("f", "Fanny", 36),
    ("g", "Gabby", 60)
  )).toDF("id", "name", "age")

    val e = sqlContext.createDataFrame(List(
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
    ("f", "c", "follow"),
    ("e", "f", "follow"),
    ("e", "d", "friend"),
    ("d", "a", "friend"),
    ("a", "e", "friend")
  )).toDF("src", "dst", "relationship")
  val g = GraphFrame(v, e)
  g.vertices.show()

    }
}
