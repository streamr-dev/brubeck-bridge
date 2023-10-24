import fetch from 'node-fetch'
import StreamrClientBrubeck, { StreamMessage as StreamMessageBrubeck, StreamPartID } from 'streamr-client-brubeck'
import StreamrClientTatum from 'streamr-client-tatum'
import { StreamMessage, EncryptedGroupKey } from 'streamr-protocol-tatum'
import crypto from 'crypto'

const BRIDGE_NODES = 10
const MY_INDEX = 0
const CHECK_INTERVAL = 15*1000

const HEX_REGEX = /^[0-9a-fA-F]+$/;



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

const TATUM_CLIENT_OPTIONS = {
    metrics: false,
    /*auth: {
        privateKey: 'NODE_PRIVATE_KEY'
    },*/
    network: {
        controlLayer: {
            entryPoints: [
                {
                    id: 'e1',
                    websocket: {
                        host: 'entrypoint-1.streamr.network',
                        port: 40401,
                        tls: true
                    }
                },
                {
                    id: 'e2',
                    websocket: {
                        host: 'entrypoint-2.streamr.network',
                        port: 40401,
                        tls: true
                    }
                }
            ]
        }
    },
    contracts: {
        streamRegistryChainAddress: '0x4F0779292bd0aB33B9EBC1DBE8e0868f3940E3F2',
        streamStorageRegistryChainAddress: '0xA5a2298c9b48C08DaBF5D76727620d898FD2BEc1',
        storageNodeRegistryChainAddress: '0xE6D449A7Ef200C0e50418c56F84079B9fe625199',
        mainChainRPCs: {
            name: 'mumbai',
            chainId: 80001,
            rpcs: [
                {
                    url: 'https://rpc-mumbai.maticvigil.com'
                }
            ]
        },
        streamRegistryChainRPCs: {
            name: 'mumbai',
            chainId: 80001,
            rpcs: [
                {
                    url: 'https://rpc-mumbai.maticvigil.com'
                }
            ]
        },
        theGraphUrl: 'https://api.thegraph.com/subgraphs/name/samt1803/network-subgraphs'
    }
}

type Topologies = {
    [streamPartition: string]: {
        [nodeId: string]: any
    }
}

const fetchTopologies = async (url: string): Promise<Topologies> => {
    const response = await fetch(`${url}/topology`)
    return response.json()
}

const getStreamPartitions = async (): Promise<Set<string>> => {
    const topologiesFromEachTracker: Topologies[] = await Promise.all(trackerUrls.map(url => fetchTopologies(url)))
    const streamParts: Set<string> = new Set()
    
    for (const topologies of topologiesFromEachTracker) {
        for (const streamPart of Object.keys(topologies)) {
            streamParts.add(streamPart)
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
    const streamrBrubeck = new StreamrClientBrubeck()
    await streamrBrubeck.connect()

    const streamrTatum = new StreamrClientTatum(TATUM_CLIENT_OPTIONS)
    await streamrTatum.connect()

    const brubeckNode = await streamrBrubeck.getNode()
    const tatumNode = await streamrTatum.getNode()

    let currentSubscriptions: Set<string> = new Set()

    const bridgeMessage = async (brubeckMsg: StreamMessageBrubeck) => {
        const msg: StreamMessage = new StreamMessage({
            messageId: brubeckMsg.messageId,
            prevMsgRef: brubeckMsg.prevMsgRef,
            content: HEX_REGEX.test(brubeckMsg.serializedContent) ? Buffer.from(brubeckMsg.serializedContent, 'hex') : Buffer.from(brubeckMsg.serializedContent, 'utf8'),
            messageType: brubeckMsg.messageType,
            contentType: brubeckMsg.contentType,
            encryptionType: brubeckMsg.encryptionType,
            groupKeyId: brubeckMsg.groupKeyId,
            newGroupKey: brubeckMsg.newGroupKey ? new EncryptedGroupKey(brubeckMsg.newGroupKey.groupKeyId, Buffer.from(brubeckMsg.newGroupKey.encryptedGroupKeyHex, 'hex')) : null,
            signature: Buffer.from(brubeckMsg.signature.substring(2), 'hex') // remove '0x'
        })

        //await tatumNode.broadcast(msg)
        console.log(brubeckMsg)
    }

    brubeckNode.addMessageListener(bridgeMessage)

    // TODO: I will always be in the topology of each subscribed stream, so the set will never actually be reduced
    const updateSubscriptions = async () => {
        console.log('Updating subscriptions')
        const streamParts: Set<string> = await getStreamPartitions()
        const targetSubscriptions: Set<string> = new Set(Array.from(streamParts).filter(streamPart => streamPart.indexOf('swash') >= 0))
        // const targetSubscriptions: Set<StreamPartition> = new Set(Array.from(streamParts).filter(streamPart => hash(streamPart) % BRIDGE_NODES == MY_INDEX))

        console.log(`Found ${streamParts.size} stream partitions total, assigned to me are ${targetSubscriptions.size}`)
        
        // Unsubscribe all subscriptions which are subscribed but are not in the target set
        for (const streamPart of currentSubscriptions) {
            if (!targetSubscriptions.has(streamPart)) {
                //console.log(`Leaving ${streamPart} on Brubeck`)
                //brubeckNode.unsubscribe(streamPart as StreamPartID)
                console.log(`Leaving ${streamPart} on Tatum`)
                tatumNode.leave(streamPart as StreamPartID)
            }
        }

        // Subscribe to all subscriptions which are not subscribed but are in the target set
        for (const streamPart of targetSubscriptions) {
            if (!currentSubscriptions.has(streamPart)) {
                console.log(`Joining ${streamPart} on Brubeck`)
                /*await*/ brubeckNode.subscribeAndWaitForJoin(streamPart as StreamPartID)
                //console.log(`Joining ${streamPart} on Tatum`)
                ///*await*/ tatumNode.join(streamPart as StreamPartID)
            }
        }

        currentSubscriptions = targetSubscriptions
    }

    await updateSubscriptions()
    console.log('Setting interval')
    setInterval(updateSubscriptions, CHECK_INTERVAL)
})()
