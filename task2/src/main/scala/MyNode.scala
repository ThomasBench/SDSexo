// YOUR_FULL_NAME_HERE THOMAS THEO NEHEMIE BENCHETRIT
package task2

import scala.collection.mutable.Queue
import scala.collection.mutable.Set

class MyNode(id: String, memory: Int, neighbours: Vector[String], router: Router) extends Node(id, memory, neighbours, router) {
    val STORE = "STORE"
    val STORE_SUCCESS = "STORE_SUCCESS"
    val STORE_FAILURE = "STORE_FAILURE"
    val RETRIEVE = "RETRIEVE"
    val GET_NEIGHBOURS = "GET_NEIGHBOURS"
    val NEIGHBOURS_RESPONSE = "NEIGHBOURS_RESPONSE"
    val RETRIEVE_SUCCESS = "RETRIEVE_SUCCESS"
    val RETRIEVE_FAILURE = "RETRIEVE_FAILURE"
    val INTERNAL_ERROR = "INTERNAL_ERROR"
    val USER = "USER"
    val nodes_may_fail = 4

    override def onReceive(from: String, message: Message): Message = {
        /* 
         * Called when the node receives a message from some where
         * Feel free to add more methods and code for processing more kinds of messages
         * NOTE: Remember that HOST must still comply with the specifications of USER messages
         *
         * Parameters
         * ----------
         * from: id of node from where the message arrived
         * message: the message
         *           (Process this message and decide what to do)
         *           (All communication between peers happens via these messages)
         */
        if (message.messageType == GET_NEIGHBOURS) { // Request to get the list of neighbours
            new Message(id, NEIGHBOURS_RESPONSE, neighbours.mkString(" "))
        }
        else if (message.messageType == RETRIEVE) { // Request to get the value
            // Init
            var key: String = ""
            var visited: Set[String] = Set()
            var response : Message = new Message("", "", "")
            // Get data from message
            from match {
                case USER => key = message.data
                case _ => {
                    val data = message.data.split(",")
                    key = data(0)
                    visited ++= data.tail
                }
            }
            // Start the DFS algo to find the val in the graph
            visited += id
            getKey(key) match {
                case Some(i) => response = new Message(id, RETRIEVE_SUCCESS, i)
                case None => {
                    val availableNeigh = neighbours.toSet.diff(visited).toSeq
                    if (availableNeigh.isEmpty) response = new Message(id, RETRIEVE_FAILURE)
                    else {
                        var i = 0
                        var found = false
                        while (i < availableNeigh.size && !found){
                            val current_neigh = availableNeigh(i)
                            i += 1
                            val sendData  = key + "," + visited.mkString(",")
                            val answer = router.sendMessage(id, current_neigh,new Message(id,RETRIEVE, sendData))
                            answer.messageType match{
                                case RETRIEVE_FAILURE => {
                                    found = false
                                    response = new Message(id, RETRIEVE_FAILURE)
                                }
                                case RETRIEVE_SUCCESS => {
                                    found = true
                                    response = new Message(id, RETRIEVE_SUCCESS, answer.data )
                                }
                            }
                        }
                    }
                }
            }
            response // Return the correct response message
        }
        else if (message.messageType == STORE) { // Request to store key->value
            /*
             * TODO: task 2.2
             * Change the storage algorithm below to store on the peers
             * when there isn't enough space on the HOST node.
             *
             * TODO: task 2.3
             * Change the storage algorithm below to handle nodes crashing.
             */
            var keyval: Array[String] = Array()
            var nbSeen:  Int = 0
            var tempNbSeen: Int = 0
            val nbReplicas = nodes_may_fail +1 
            var visited: Set[String] = Set()
            var response: Message = new Message("", "", "")
            from match {
                case USER => keyval = message.data.split("->")
                case _ => {
                    val data = message.data.split("/") // Nomenclature : key->val/nbSeen/visited
                    keyval = data(0).split("->")
                    nbSeen += data(1).toInt
                    visited ++= data(2).split(",")
                }
            }
            tempNbSeen += nbSeen
            visited += id
            val localStore = setKey(keyval(0),keyval(1))
            if (localStore) tempNbSeen+=1

            val availableNeigh = neighbours.toSet.diff(visited).toSeq
            if (availableNeigh.isEmpty) response = new Message(id, STORE_FAILURE)
            else {
                var i: Int = 0
                while (i < availableNeigh.size && tempNbSeen < nbReplicas){
                    val current_neigh = availableNeigh(i)
                    val sendData = keyval.mkString("->") + "/" + tempNbSeen.toString + "/" + visited.mkString(",")
                    i += 1
                    val answer = router.sendMessage(id, current_neigh,new Message(id,STORE, sendData))
                    if (answer.messageType == STORE_SUCCESS) {
                        val data = answer.data.split("/") // Nomenclature : key->val/tempNbSeen/visited
                        tempNbSeen += data(1).toInt
                        visited ++= data(2).split(",")
                        }
                    }
                }
            val sendData = keyval.mkString("->") + "/" + tempNbSeen.toString + "/" + visited.mkString(",")
            from match {
                case USER => response = if (tempNbSeen < nbReplicas) new Message(id, STORE_FAILURE) else new Message(id, STORE_SUCCESS)
                case _ => response = if (tempNbSeen == nbSeen) new Message(id, STORE_FAILURE) else new Message(id, STORE_SUCCESS,sendData)
            }
            response
        }

        /*
         * Feel free to add more kinds of messages.
         */
        else
            new Message(id, INTERNAL_ERROR)
    }
}