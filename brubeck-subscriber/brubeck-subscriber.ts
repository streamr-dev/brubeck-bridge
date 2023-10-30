import fetch from 'node-fetch'
import StreamrClient, { StreamMessage, StreamPartID } from 'streamr-client'
import crypto from 'crypto'
import ipc from 'node-ipc'
import { MessageBetweenInstances } from '../common/messageTypes'

const BRIDGE_NODES = parseInt(process.env['BRIDGE_NODES'] || '10')
const MY_INDEX = parseInt(process.env['BRIDGE_MY_INDEX'] || '0')
const CHECK_INTERVAL = parseInt(process.env['BRIDGE_TRACKER_INTERVAL'] || '15000')

ipc.config.id = `brubeck-${MY_INDEX}`
ipc.config.retry = 1500
ipc.config.silent = true

const trackerUrls = [
    'https://brubeck3.streamr.network:30401',
    'https://brubeck3.streamr.network:30402',
    'https://brubeck3.streamr.network:30403',
    'https://brubeck3.streamr.network:30404',
    'https://brubeck3.streamr.network:30405',
    'https://brubeck4.streamr.network:30401',
    'https://brubeck4.streamr.network:30402',
    'https://brubeck4.streamr.network:30403',
    'https://brubeck4.streamr.network:30404',
    'https://brubeck4.streamr.network:30405',
]

type Topologies = {
    [streamPartition: string]: {
        [nodeId: string]: any
    }
}

const fetchTopologies = async (url: string): Promise<Topologies> => {
    const response = await fetch(`${url}/topology`)
    return response.json()
}

const getStreamPartitions = async (myNodeId: string): Promise<Set<string>> => {
    const topologiesFromEachTracker: Topologies[] = await Promise.all(trackerUrls.map(url => fetchTopologies(url)))
    const streamParts: Set<string> = new Set()
    
    for (const topologies of topologiesFromEachTracker) {
        for (const streamPart of Object.keys(topologies)) {
            // Don't add streamParts where I am the only participant
            if (topologies[streamPart].length > 1 || topologies[streamPart][0] !== myNodeId) {
                streamParts.add(streamPart)
            } else {
                console.log(`Skipping ${streamPart} because I'm the only one there`)
            }
        }
    }

    return streamParts
}

const hash = (streamPart: string): number => {
    const hashBuffer = crypto.createHash('sha256').update(streamPart).digest()
    return hashBuffer.readUIntBE(0, 6)
}

/**
StreamMessage {
  messageId: MessageID {
    streamId: 'swashdu.eth/general',
    streamPartition: 0,
    timestamp: 1698159348713,
    sequenceNumber: 0,
    publisherId: '0x193ff314eeb7657668103c0d6fab7271f8c1fc78',
    msgChainId: '0e046f99-9adb-436d-a027-78807533089d'
  },
  prevMsgRef: MessageRef { timestamp: 1698159348078, sequenceNumber: 0 },
  messageType: 27,
  contentType: 0,
  encryptionType: 2,
  groupKeyId: '6879fa24-0d09-40fc-8c68-b6964aaa26fa-GroupKey2',
  newGroupKey: null,
  signature: '0xe03e32dc1d2e07c8a715fbde49166cef9dd522f6ac6b6f1e05bdc0de2976c86e4b48d864d980b7fb40fdb8931dddfa064b6d1014b14315a6dcb48d776910625a1c',
  serializedContent: '5e57ed3269a2b0776edbbfb8a027633371ba5fcba2b6d9126a0e7bd1f5feb809b7835b50cdf0ab4bbad9e106341a3b7e51f98f39336195d026a711a5676234c6dffb924995fb736d62dffbf066f6de153157fdccce79049b42d033de55a8fa91c1202a625bd87943dc2b1672c10f9b35d4e08acca7600f7414dea40ad6a0c372f3fdebd3f6466f1a2742f38b002f2d4396322a9bbb369676982e9e7ed936cc695da55992bc6ef58b36849aaaa0b315ac8745ef7cd7d4a37d79989fd0aaa687515898c3a0381d8ce56fd4ce1df3aa6d4fdc6bb5b87a1f09d14733921328d0130d25bd9565d3063e7c826f596e501795d609ca4896506de2e9f4b79136dd0b71468ff075df7667645fea2b255212fad9f1953570fe6b5e8b1767563584b917983c111577e38b3c954a641066d8ae87240d83a1c044e78410c2ca774dde524f48a06db20cb57b14b3fa7578a27b10f2d9c69ccb6ff57ccd7c10871db42e215e8c74a0863eb264d206746efbc708906ae6129bfd8672a7fbc99ab4f913dac7577f932206683e82b145c83fa74f50ab26de7fcaf816895cd3dbd979023c1796a8d869497b46035afc3db0e39443719c368dc4ceff1c4086eca8926d4f88e4618b83c1354662116310028e29611ca9150c487dc6e42045848bcec842873ab8aad25b4980b1de035fc6a8ad93bf4b8632d5ca9599fe5714be4e7079f6a0b5eb6500a97f4591059ef3d45f7389e98d4a16afcb93e71eec8ff2fe270957cd43bece2ced46d4eb9e22205c37f0c6c3380fa63a4b25bb2b22d7503feaa0f4bd47765d12e149712d773e29b096e71b6e201db79062ffa37bfae5554f15bfdaf6d5df124e6daa74019a908c776417fb9c76c41f7f2daf8cd5bb7a63cc52fbcc40bf9bb51a1e382e48b6b9df783c95af3fd26e580726e3941f300efb0984c06b9435c460ecb339d238bf9607467f1c7c6f405252e2d4756a3da1f990d27239c2e6d60d3dfebe8251efe227d190d138f5820784c557d437ce8655f36b60367d867dfb60c593b99e592a2ed6a288bc01137a6bb0b319d1fba4e81b38863d6c9ef4409048bde23f032cf3602359abe1aef0416d7f50d94c3464ff060d263afed1cc461b7848ba549d16e2c491d396bb7ece191b793d2b13e5d3dcf8c41e33'
}
 */
;(async () => {
    const streamr = new StreamrClient()
    await streamr.connect()
    const node = await streamr.getNode()

    let currentSubscriptions: Set<string> = new Set()

    // Connect to tatum-publisher over local unix socket
    const socketName = `tatum-${MY_INDEX}`
    let socketConnected = false
    ipc.connectTo(
        socketName,
        function () {
            ipc.of[socketName].on(
                'connect',
                function() {
                    socketConnected = true
                    console.log(`Connected to ${socketName}`)
                }
            )
            ipc.of[socketName].on(
                'disconnect',
                function() {
                    socketConnected = false
                    console.log(`Disconnected from ${socketName}`)
                }
            )
        }
    )

    const bridgeMessage = async (msg: StreamMessage) => {
        const serialized: MessageBetweenInstances = {
            msg: {
                messageId: {
                    streamId: msg.messageId.streamId,
                    streamPartition: msg.messageId.streamPartition,
                    timestamp: msg.messageId.timestamp,
                    sequenceNumber: msg.messageId.sequenceNumber,
                    publisherId: msg.messageId.publisherId,
                    msgChainId: msg.messageId.msgChainId,
                },
                prevMsgRef: msg.prevMsgRef ? { 
                    timestamp: msg.prevMsgRef.timestamp, 
                    sequenceNumber: msg.prevMsgRef.sequenceNumber,
                } : null,
                messageType: msg.messageType,
                contentType: msg.contentType,
                encryptionType: msg.encryptionType,
                groupKeyId: msg.groupKeyId,
                newGroupKey: msg.newGroupKey ? msg.newGroupKey.serialize() : null,
                signature: msg.signature,
                serializedContent: msg.serializedContent,                
            }
        }
        if (socketConnected) {
            ipc.of[socketName].emit(
                'message',
                JSON.stringify(serialized)
            )
        }
    }

    node.addMessageListener(bridgeMessage)

    const updateSubscriptions = async () => {
        console.log(`Updating subscriptions (my nodeId: ${node.getNodeId()})`)
        const streamParts: Set<string> = await getStreamPartitions(node.getNodeId())
        // const targetSubscriptions: Set<string> = new Set(['streamr.eth/metrics/nodes/firehose/sec#83'])
        const targetSubscriptions: Set<string> = new Set(Array.from(streamParts).filter(streamPart => hash(streamPart) % BRIDGE_NODES == MY_INDEX))

        console.log(`Found ${streamParts.size} stream partitions total, assigned to me are ${targetSubscriptions.size}`)
        
        // Unsubscribe all subscriptions which are subscribed but are not in the target set
        for (const streamPart of currentSubscriptions) {
            if (!targetSubscriptions.has(streamPart)) {
                console.log(`Leaving ${streamPart}`)
                node.unsubscribe(streamPart as StreamPartID)
            }
        }

        // Subscribe to all subscriptions which are not subscribed but are in the target set
        for (const streamPart of targetSubscriptions) {
            if (!currentSubscriptions.has(streamPart)) {
                console.log(`Joining ${streamPart}`)
                node.subscribe(streamPart as StreamPartID)
            }
        }

        currentSubscriptions = targetSubscriptions
        if (socketConnected) {
            const msg: MessageBetweenInstances = {
                streamParts: Array.from(currentSubscriptions)
            }
            ipc.of[socketName].emit(
                'message',
                JSON.stringify(msg)
            )
        }
    }

    await updateSubscriptions()

    console.log('Setting interval')
    setInterval(updateSubscriptions, CHECK_INTERVAL)
})()
