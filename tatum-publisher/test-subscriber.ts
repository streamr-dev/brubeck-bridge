import StreamrClient from 'streamr-client'

const streamr = new StreamrClient()

streamr.subscribe({
    streamId: 'eth-watch.eth/ethereum/blocks',
    //streamId: 'streams.dimo.eth/firehose/weather',
    partition: 0
}, (msg) => {
    console.log(msg)
})