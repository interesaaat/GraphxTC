/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.lib

import org.apache.spark.graphx._
import org.apache.spark.util.collection.OpenHashSet

import scala.reflect.ClassTag

/**
 * Compute the number of shared vertexes between two unconnected vertexes.
 */
object TransitiveClosure {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) = {

    // Construct set representations of the exit rule
    val tcGraph = graph.mapVertices { case (vid, attr) =>
        val set1 = new OpenHashSet[VertexId]
        val set2 = new OpenHashSet[VertexId]
        set1.add(vid)
        (set1, set2)
      }

    val initial = new OpenHashSet[VertexId]

    def sendMessage(triplet: EdgeTriplet[(OpenHashSet[VertexId], OpenHashSet[VertexId]), ED]): Iterator[(VertexId, OpenHashSet[VertexId])] = {
     // println(triplet)
      if(triplet.srcAttr._2.size != 0) {
        val send = new OpenHashSet[VertexId]
       triplet.dstAttr
        triplet.srcAttr._2.iterator.foreach(v => if (!triplet.dstAttr._1.contains(v)) send.add(v))
        //  }
        //println(triplet.dstId)
        //send.iterator.foreach(println)
        if (send.size != 0) {
          Iterator((triplet.dstId, send))
        } else {
          Iterator.empty
        }
      } else {
       Iterator.empty
     }
    }

    initial.add(Long.MinValue)
    val tc = tcGraph.pregel(initial, activeDirection = EdgeDirection.Either)(
      (id, attr, msg) => {
          val set2 = new OpenHashSet[VertexId]
          msg.iterator.foreach(m => if(m == Long.MinValue) set2.add(id) else if (!attr._1.contains(m)) set2.add(m))
          //set2.iterator.foreach(println)
          (attr._1.union(set2), set2)
      },
     sendMsg = sendMessage,
      (a,b) => a.union(b))

    tc.vertices.flatMap {
      case (vid, data) => data._1.iterator.map(x => (x, vid))
      }
  }
}
