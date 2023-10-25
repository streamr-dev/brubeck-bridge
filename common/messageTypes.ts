export type MessageBetweenInstances = {
    msg?: Message
    streamParts?: string[]
}

export type Message = {
    messageId: {
      streamId: string,
      streamPartition: number,
      timestamp: number,
      sequenceNumber: number,
      publisherId: string,
      msgChainId: string,
    },
    prevMsgRef: { 
        timestamp: number, 
        sequenceNumber: number,
    } | null,
    messageType: number,
    contentType: number,
    encryptionType: number,
    groupKeyId: string | null,
    newGroupKey: string | null,
    signature: string,
    serializedContent: string,
}
