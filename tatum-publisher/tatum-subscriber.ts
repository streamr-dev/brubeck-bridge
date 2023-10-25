import StreamrClient, { StreamMessage, StreamPartID } from 'streamr-client'
import crypto from 'crypto'
import ipc from 'node-ipc'
import { MessageBetweenInstances, Message } from '../common/messageTypes'

const BRIDGE_NODES = parseInt(process.env['BRIDGE_NODES'] || '10')
const MY_INDEX = parseInt(process.env['BRIDGE_MY_INDEX'] || '0')
const CHECK_INTERVAL = parseInt(process.env['BRIDGE_TRACKER_INTERVAL'] || '15000')

ipc.config.id = `tatum-${MY_INDEX}`
ipc.config.retry = 1500

const socketName = `tatum-${MY_INDEX}`

ipc.serve(
    function() {
        ipc.server.on(
            'message',
            function(data) {
                console.log(data)
            }
        )
        ipc.server.on(
            'socket.disconnected',
            function(socket, destroyedSocketID) {
                console.log(`Client ${destroyedSocketID} disconnected!`);
            }
        )
    }
)

ipc.server.start()