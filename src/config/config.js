const os = require("os")

const isDevTunnel = () => {
  return process.env.MEDIASOUP_ANNOUNCED_IP && process.env.MEDIASOUP_ANNOUNCED_IP.includes("devtunnels.ms")
}

const getAnnouncedIP = () => {
  if (process.env.MEDIASOUP_ANNOUNCED_IP) {
    return process.env.MEDIASOUP_ANNOUNCED_IP
  }

  if (process.env.EXTERNAL_IP) {
    return process.env.EXTERNAL_IP
  }

  return "127.0.0.1"
}

const config = {
  mediasoup: {
    numWorkers: Math.min(Object.keys(os.cpus()).length, 4),

    worker: {
      rtcMinPort: Number.parseInt(process.env.MEDIASOUP_MIN_PORT) || 10000,
      rtcMaxPort: Number.parseInt(process.env.MEDIASOUP_MAX_PORT) || 10100,
      logLevel: process.env.NODE_ENV === "production" ? "warn" : "debug",
      logTags: ["info", "ice", "dtls", "rtp", "srtp", "rtcp"],
    },

    router: {
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
        {
          kind: "video",
          mimeType: "video/VP9",
          clockRate: 90000,
          parameters: {
            "profile-id": 2,
            "x-google-start-bitrate": 1000,
          },
        },
        {
          kind: "video",
          mimeType: "video/h264",
          clockRate: 90000,
          parameters: {
            "packetization-mode": 1,
            "profile-level-id": "4d0032",
            "level-asymmetry-allowed": 1,
            "x-google-start-bitrate": 1000,
          },
        },
        {
          kind: "video",
          mimeType: "video/h264",
          clockRate: 90000,
          parameters: {
            "packetization-mode": 1,
            "profile-level-id": "42e01f",
            "level-asymmetry-allowed": 1,
            "x-google-start-bitrate": 1000,
          },
        },
      ],
    },

    webRtcTransport: {
      listenIps: [
        {
          ip: "0.0.0.0",
          announcedIp: getAnnouncedIP(),
        },
      ],
      maxIncomingBitrate: 1500000,
      initialAvailableOutgoingBitrate: 1000000,
      enableUdp: !isDevTunnel(),
      enableTcp: true,
      preferUdp: true,
      preferTcp: isDevTunnel(), 
      enableSctp: true, 
    },
  },

  // Server settings
  server: {
    port: process.env.PORT || 3001,
    cors: {
      origin: process.env.CLIENT_URL || "*",
      methods: ["GET", "POST"],
      credentials: true,
    },
  },

  external: {
    isDevTunnel: isDevTunnel(),
    announcedIp: getAnnouncedIP(),
    listenIp: "0.0.0.0",
  },
}

console.log(" Mediasoup Configuration:")
console.log(`   Listen IP: ${config.mediasoup.webRtcTransport.listenIps[0].ip}`)
console.log(`   Announced IP: ${config.mediasoup.webRtcTransport.listenIps[0].announcedIp}`)
console.log(`   Dev Tunnel Mode: ${config.external.isDevTunnel}`)
console.log(`   RTC Port Range: ${config.mediasoup.worker.rtcMinPort}-${config.mediasoup.worker.rtcMaxPort}`)
console.log(`   Transport Mode: ${isDevTunnel() ? "TCP-Only (Tunnel)" : "UDP+TCP"}`)

module.exports = config
