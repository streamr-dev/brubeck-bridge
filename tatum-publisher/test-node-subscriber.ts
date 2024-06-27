import StreamrClient from '@streamr/sdk'
import { StreamMessage, toStreamID, toStreamPartID } from '@streamr/protocol';

(async () => {
    const streamr = new StreamrClient()
    await streamr.connect()
    const node = await streamr.getNode()

    node.addMessageListener((msg: StreamMessage) => {
        console.log(msg)
        console.log(`Content: ${new Buffer(msg.content).toString('utf-8')}`)
    })

    await node.join(toStreamPartID(toStreamID('streams.dimo.eth/vehicles/21957'), 0))
})()
