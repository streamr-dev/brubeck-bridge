import StreamrClient from 'streamr-client'
import { TATUM_CLIENT_OPTIONS } from './tatum-client-options'

const streamr = new StreamrClient(TATUM_CLIENT_OPTIONS)

streamr.subscribe({
    streamId: 'streamr.eth/metrics/nodes/firehose/sec',
    partition: 43
}, (msg) => {
    console.log(msg)
})