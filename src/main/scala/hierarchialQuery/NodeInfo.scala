/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.scala.hierarchialQuery

import org.apache.spark.graphx.VertexId

//node information defined,  which needs to be communicated through nodes
//case class NodeInfo (specificNodeId: VertexId, specificNodeIdType: Int,  spDistance: Long, spNumber: Long, hierLevelDifference: Long, 
//                     closenessNodeScore: Double, parentId: VertexId, vistiedColor: Int)


case class NodeInfo (specificNodeId: VertexId, specificNodeIdType: Int,  spDistance: Long, spNumber: Long, hierLevelDifference: Long, 
                     closenessNodeScore: Double, parentId: VertexId, visitedColor: Int, lowerBoundCloseScore: Double, upperBoundCloseScore: Double)

/*var specificNdId: VertexId = specificNodeId             //source node Id for bfs
 var spDist: Long = spDistance                    //shortest path distance from srcndId
 var spNum: Long = spNumber                    //shortest path number from srcndId
 var hierDiff: Long = hierDifference            //hierarchical level difference from specific node
 var closenessScore: Double = closenessNodeScore
 var parentNdId: VertexId = parentId
 
  def updateSpDist(newSpDist: Long) = {
    spDist = spDist + 1
  }*/
 
//enum type of node types
object nodeTypeProductEnum extends Enumeration {
        type nodeType = Value
        val PRODUCT, VULNERABILITY, BUGID, WORKAROUND, TECHNOLOGY, WORKGROUP, PRODUCTSITE = Value
    }
//PRODUCT.id : get the node enum value ;       0, 1, 2, 3, 4, 5.....


//enum type of node types
object nodeTypeDBLPEnum extends Enumeration {
        type nodeType = Value
        val DUMMY, PEOPLE, PAPER, TOPIC, DATE, ARTICLE, BOOK, INCOLLECTION, INPROCEEDING, MASTERSTHESIS, PHDTHESIS, PROCEEDINGS, WWW = Value
    }
//PRODUCT.id : get the node enum value ;       0, 1, 2, 3, 4, 5.....


//enum type of node VistiedColor      //visited or not.  WHITE : 0 no visited. 
//grey: visiting (current node visited, not all its nieghbor visited);   black vistied (all neighbors); RED means not to continue from this point for bounding

object visitedColorEnum extends Enumeration {
        type visitColor = Value
        val WHITE, GREY, BLACK, RED = Value           //0, 1, 2, 3...
    }
  
//for synthetic graph node types enum
object nodeTypeSyntheticGraphEnum extends Enumeration {
        type nodeType = Value
        val TYPE0HIER, TYPE1HIER, TYPE0INHERIT, TYPE1INHERIT, TYPE0GENERIC, TYPE1GENERIC, TYPE2GENERIC = Value
    }
    
