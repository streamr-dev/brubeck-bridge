import StreamrClient, { EthereumAddress, StreamPartID, StreamID } from 'streamr-client'
import { StreamMessage, MessageID, MessageRef, EncryptedGroupKey } from '@streamr/protocol'
import { hexToBinary } from '@streamr/utils'
import ipc from 'node-ipc'
import { MessageBetweenInstances } from '../common/messageTypes'
import { TATUM_CLIENT_OPTIONS } from './tatum-client-options'

const MY_INDEX = parseInt(process.env['BRIDGE_MY_INDEX'] || '0')

ipc.config.id = `tatum-${MY_INDEX}`
ipc.config.retry = 1500
ipc.config.silent = true

;(async () => {
    const streamr = new StreamrClient(TATUM_CLIENT_OPTIONS)
    await streamr.connect()
    const node = await streamr.getNode()

    let currentSubscriptions: Set<string> = new Set()

    const updateSubscriptions = async (targetSubscriptions: Set<string>) => {
        console.log('Updating subscriptions')
        console.log(`Got ${targetSubscriptions.size} stream partitions from Brubeck`)
        
        // Unsubscribe all subscriptions which are subscribed but are not in the target set
        for (const streamPart of currentSubscriptions) {
            if (!targetSubscriptions.has(streamPart)) {
                console.log(`Leaving ${streamPart}`)
                node.leave(streamPart as StreamPartID)
            }
        }

        // Subscribe to all subscriptions which are not subscribed but are in the target set
        for (const streamPart of targetSubscriptions) {
            if (!currentSubscriptions.has(streamPart)) {
                console.log(`Joining ${streamPart}`)
                node.join(streamPart as StreamPartID)
            }
        }

        currentSubscriptions = targetSubscriptions
    }

    ipc.serve(
        function() {
            ipc.server.on(
                'message',
                function(data) {
                    const msgFromBrubeck: MessageBetweenInstances = JSON.parse(data)
                    if (msgFromBrubeck.msg) {
                        let parsedNewGroupKey: [string, string] | null = null
                        if (msgFromBrubeck.msg.newGroupKey) {
                            parsedNewGroupKey = JSON.parse(msgFromBrubeck.msg.newGroupKey)
                        }

                        const message: StreamMessage = new StreamMessage({
                            messageId: new MessageID(
                                msgFromBrubeck.msg.messageId.streamId as StreamID,
                                msgFromBrubeck.msg.messageId.streamPartition,
                                msgFromBrubeck.msg.messageId.timestamp,
                                msgFromBrubeck.msg.messageId.sequenceNumber,
                                msgFromBrubeck.msg.messageId.publisherId as EthereumAddress,
                                msgFromBrubeck.msg.messageId.msgChainId,
                            ),
                            prevMsgRef: msgFromBrubeck.msg.prevMsgRef ? new MessageRef( 
                                msgFromBrubeck.msg.prevMsgRef.timestamp, 
                                msgFromBrubeck.msg.prevMsgRef.sequenceNumber,
                            ) : null,
                            messageType: msgFromBrubeck.msg.messageType,
                            contentType: msgFromBrubeck.msg.contentType,
                            encryptionType: msgFromBrubeck.msg.encryptionType,
                            groupKeyId: msgFromBrubeck.msg.groupKeyId,
                            newGroupKey: parsedNewGroupKey ? new EncryptedGroupKey(parsedNewGroupKey[0], Buffer.from(parsedNewGroupKey[1], 'hex')) : null,
                            signature: hexToBinary(msgFromBrubeck.msg.signature),
                            content: msgFromBrubeck.msg.encryptionType === 0 ? 
                                Buffer.from(msgFromBrubeck.msg.serializedContent, 'utf8') : Buffer.from(msgFromBrubeck.msg.serializedContent, 'hex')
                        })
                        node.broadcast(message)
                        // console.log(msgFromBrubeck.msg)
                    } else if (msgFromBrubeck.streamParts) {
                        updateSubscriptions(new Set<string>(msgFromBrubeck.streamParts))
                    } else {
                        console.error('Unexpected message received from Brubeck:', msgFromBrubeck)
                    }
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

})()
