const mediasoup = require("mediasoup")

class SFUManager {
  constructor() {
    this.worker = null
    this.routers = new Map()
    this.transports = new Map()
    this.producers = new Map() 

    this.consumers = new Map() 
    this.rooms = new Map()
  }

  async initialize() {
    console.log("Initializing SFU Manager...")

    try {
      this.worker = await mediasoup.createWorker({
        logLevel: "warn",
        logTags: ["info", "ice", "dtls", "rtp", "srtp", "rtcp"],
        rtcMinPort: process.env.MEDIASOUP_MIN_PORT || 10000,
        rtcMaxPort: process.env.MEDIASOUP_MAX_PORT || 10100,
      })

      this.worker.on("died", (error) => {
        console.error("Mediasoup worker died:", error)
        process.exit(1)
      })

      console.log("SFU Manager initialized successfully")
    } catch (error) {
      console.error("Failed to initialize SFU Manager:", error)
      throw error
    }
  }

  async createRoom(roomId) {
    if (this.routers.has(roomId)) {
      return this.routers.get(roomId)
    }

    console.log(`Creating SFU room: ${roomId}`)

    try {
      const router = await this.worker.createRouter({
        mediaCodecs: [
          {
            kind: "audio",
            mimeType: "audio/opus",
            clockRate: 48000,
            channels: 2,
          },
          {
            kind: "video",
            mimeType: "video/VP8",
            clockRate: 90000,
            parameters: {
              "x-google-start-bitrate": 1000,
            },
          },
        ],
      })

      this.routers.set(roomId, router)
      this.rooms.set(roomId, {
        id: roomId,
        participants: new Map(),
        createdAt: new Date(),
      })

      router.on("workerclose", () => {
        this.routers.delete(roomId)
        this.rooms.delete(roomId)
      })

      console.log(`SFU room created: ${roomId}`)
      return router
    } catch (error) {
      console.error(`Error creating SFU room ${roomId}:`, error)
      throw error
    }
  }

  async createWebRtcTransport(roomId, participantId, direction) {
    const router = this.routers.get(roomId)
    if (!router) {
      throw new Error(`Router not found for room: ${roomId}`)
    }

    console.log(`Creating ${direction} transport for ${participantId} in room ${roomId}`)

    try {
      const transport = await router.createWebRtcTransport({
        listenIps: [
          {
            ip: "0.0.0.0",
            announcedIp: process.env.MEDIASOUP_ANNOUNCED_IP || "127.0.0.1",
          },
        ],
        enableUdp: true,
        enableTcp: true,
        preferUdp: true,
        initialAvailableOutgoingBitrate: 1000000,
        minimumAvailableOutgoingBitrate: 600000,
        maxSctpMessageSize: 262144,
        // Add additional options for better NAT traversal
        webRtcServerOptions: {
          listenInfos: [
            {
              protocol: "udp",
              ip: "0.0.0.0",
              announcedIp: process.env.MEDIASOUP_ANNOUNCED_IP || "127.0.0.1",
            },
            {
              protocol: "tcp",
              ip: "0.0.0.0",
              announcedIp: process.env.MEDIASOUP_ANNOUNCED_IP || "127.0.0.1",
            },
          ],
        },
      })

      const transportInfo = {
        id: transport.id,
        transport,
        roomId,
        participantId,
        direction,
        createdAt: new Date(),
      }

      this.transports.set(transport.id, transportInfo)

      transport.on("dtlsstatechange", (dtlsState) => {
        console.log(`Transport ${transport.id} DTLS state: ${dtlsState}`)
        if (dtlsState === "closed") {
          this.transports.delete(transport.id)
        }
      })

      // Add more detailed logging
      transport.on("icestatechange", (iceState) => {
        console.log(`Transport ${transport.id} ICE state: ${iceState}`)
      })

      transport.on("sctpstatechange", (sctpState) => {
        console.log(`Transport ${transport.id} SCTP state: ${sctpState}`)
      })

      return {
        id: transport.id,
        iceParameters: transport.iceParameters,
        iceCandidates: transport.iceCandidates,
        dtlsParameters: transport.dtlsParameters,
        sctpParameters: transport.sctpParameters,
      }
    } catch (error) {
      console.error(` Error creating transport:`, error)
      throw error
    }
  }

  async connectTransport(transportId, dtlsParameters) {
    const transportInfo = this.transports.get(transportId)
    if (!transportInfo) {
      throw new Error(`Transport not found: ${transportId}`)
    }

    try {
      console.log(`Connecting transport ${transportId} with DTLS parameters:`, JSON.stringify(dtlsParameters))
      await transportInfo.transport.connect({ dtlsParameters })
      console.log(` Transport connected: ${transportId}`)
    } catch (error) {
      console.error(`Error connecting transport ${transportId}:`, error)

      // Try to recover if possible
      if (error.message.includes("already connected")) {
        console.log(` Transport ${transportId} was already connected, ignoring error`)
        return // Return successfully if it was already connected
      }

      throw error
    }
  }

  async createProducer(transportId, kind, rtpParameters) {
    const transportInfo = this.transports.get(transportId)
    if (!transportInfo) {
      throw new Error(`Transport not found: ${transportId}`)
    }

    try {
      const producer = await transportInfo.transport.produce({
        kind,
        rtpParameters,
      })

      const producerInfo = {
        id: producer.id,
        producer,
        kind,
        roomId: transportInfo.roomId,
        participantId: transportInfo.participantId,
        createdAt: new Date(),
      }

      this.producers.set(producer.id, producerInfo)

      producer.on("transportclose", () => {
        this.producers.delete(producer.id)
      })

      console.log(`Producer created: ${kind} (${producer.id}) for ${transportInfo.participantId}`)
      return producer.id
    } catch (error) {
      console.error(`Error creating producer:`, error)
      throw error
    }
  }

  async createConsumer(transportId, producerId, rtpCapabilities) {
    const transportInfo = this.transports.get(transportId)
    const producerInfo = this.producers.get(producerId)

    if (!transportInfo || !producerInfo) {
      throw new Error("Transport or producer not found")
    }

    const router = this.routers.get(transportInfo.roomId)
    if (!router.canConsume({ producerId, rtpCapabilities })) {
      throw new Error("Cannot consume")
    }

    try {
      const consumer = await transportInfo.transport.consume({
        producerId,
        rtpCapabilities,
        paused: true,
      })
      console.log(consumer,"consumerconsumer");
      

      const consumerInfo = {
        id: consumer.id,
        consumer,
        producerId,
        roomId: transportInfo.roomId,
        participantId: transportInfo.participantId,
        producerParticipantId: producerInfo.participantId,
        createdAt: new Date(),
      }
      console.log(consumerInfo,"consumerInfo");
      

      this.consumers.set(consumer.id, consumerInfo)

      consumer.on("transportclose", () => {
        this.consumers.delete(consumer.id)
      })

      consumer.on("producerclose", () => {
        this.consumers.delete(consumer.id)
      })

      console.log(`Consumer created: ${consumer.id} for ${transportInfo.participantId}`)

      return {
        id: consumer.id,
        producerId,
        kind: consumer.kind,
        rtpParameters: consumer.rtpParameters,
        producerParticipantId: producerInfo.participantId,
      }
    } catch (error) {
      console.error(` Error creating consumer:`, error)
      throw error
    }
  }

  async resumeConsumer(consumerId) {
    const consumerInfo = this.consumers.get(consumerId)
    if (!consumerInfo) {
      throw new Error(`Consumer not found: ${consumerId}`)
    }

    try {
      await consumerInfo.consumer.resume()
      console.log(`Consumer resumed: ${consumerId}`)
    } catch (error) {
      console.error(` Error resuming consumer:`, error)
      throw error
    }
  }

  getRouterRtpCapabilities(roomId) {
    const router = this.routers.get(roomId)
    if (!router) {
      throw new Error(`Router not found for room: ${roomId}`)
    }
    return router.rtpCapabilities
  }

  getProducersInRoom(roomId, excludeParticipantId = null) {
    const producers = []
    for (const [producerId, producerInfo] of this.producers.entries()) {
      if (producerInfo.roomId === roomId && producerInfo.participantId !== excludeParticipantId) {
        producers.push({
          id: producerId,
          participantId: producerInfo.participantId,
          kind: producerInfo.kind,
        })
      }
    }
    return producers
  }

  addParticipantToRoom(roomId, participantId, participantInfo) {
    const room = this.rooms.get(roomId)
    if (room) {
      room.participants.set(participantId, {
        id: participantId,
        ...participantInfo,
        joinedAt: new Date(),
      })
    }
  }

  removeParticipantFromRoom(roomId, participantId) {
    const room = this.rooms.get(roomId)
    if (room) {
      room.participants.delete(participantId)
    }

    // Clean up participant's transports, producers, and consumers
    for (const [transportId, transportInfo] of this.transports.entries()) {
      if (transportInfo.participantId === participantId && transportInfo.roomId === roomId) {
        transportInfo.transport.close()
        this.transports.delete(transportId)
      }
    }

    for (const [producerId, producerInfo] of this.producers.entries()) {
      if (producerInfo.participantId === participantId && producerInfo.roomId === roomId) {
        producerInfo.producer.close()
        this.producers.delete(producerId)
      }
    }

    for (const [consumerId, consumerInfo] of this.consumers.entries()) {
      if (consumerInfo.participantId === participantId && consumerInfo.roomId === roomId) {
        consumerInfo.consumer.close()
        this.consumers.delete(consumerId)
      }
    }

    console.log(`🧹 Cleaned up participant ${participantId} from room ${roomId}`)
  }

  async closeRoom(roomId) {
    console.log(`Closing SFU room: ${roomId}`)

    const router = this.routers.get(roomId)
    if (router) {
      router.close()
      this.routers.delete(roomId)
    }

    this.rooms.delete(roomId)
    console.log(`SFU room closed: ${roomId}`)
  }

  getRoomStats(roomId) {
    const room = this.rooms.get(roomId)
    if (!room) return null

    return {
      roomId,
      participantCount: room.participants.size,
      createdAt: room.createdAt,
    }
  }
}

module.exports = new SFUManager()
