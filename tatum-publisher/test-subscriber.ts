import StreamrClient from '@streamr/sdk'

const streamr = new StreamrClient()

streamr.subscribe({
    // streamId: 'eth-watch.eth/ethereum/blocks',
    streamId: 'eth-watch.eth/ethereum/events',
    //streamId: 'streams.dimo.eth/firehose/weather',
    partition: 0
}, (msg) => {
    console.log(msg)
})