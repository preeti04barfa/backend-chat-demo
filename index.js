const express = require("express")
const http = require("http")
const socketIo = require("socket.io")
const cors = require("cors")
const mediasoup = require("mediasoup")
require("dotenv").config()

const app = express()
const server = http.createServer(app)

app.use(
  cors({
    origin: "*",
    methods: ["GET", "POST"],
    credentials: true,
  }),
)

app.use(express.json())

const io = socketIo(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
    credentials: true,
  },
  pingTimeout: 60000,
  pingInterval: 25000,
})

const dbConnection = require("./src/config/Db.config")

const CallHistory = require("./src/models/CallHistory")
const GroupChat = require("./src/models/GroupChat")
const Group = require("./src/models/Group")
const Chat = require("./src/models/Chat")
const User = require("./src/models/User")

// Global variables
const connectedUsers = new Map()
const activeCalls = new Map()
const callTimeouts = new Map()

// mediasoup objects (only for group calls)
const workers = []
const numWorkers = Object.keys(require("os").cpus()).length
const routers = new Map() // callId -> router
const transports = new Map() // userId_callId -> transport
const producers = new Map() // userId_callId -> Map(kind -> producer)
const consumers = new Map() // userId_callId -> Map(producerId -> consumer)

// mediasoup settings
const mediasoupSettings = {
  worker: {
    rtcMinPort: 10000,
    rtcMaxPort: 10100,
    logLevel: "warn",
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
    ],
  },
  webRtcTransport: {
    listenIps: [
      {
        ip: "0.0.0.0",
        announcedIp: process.env.ANNOUNCED_IP || "127.0.0.1", // Replace with your public IP in production
      },
    ],
    initialAvailableOutgoingBitrate: 1000000,
    minimumAvailableOutgoingBitrate: 600000,
    maxSctpMessageSize: 262144,
    maxIncomingBitrate: 1500000,
  },
}

// Initialize mediasoup workers
async function initializeMediasoupWorkers() {
  console.log(`Initializing ${numWorkers} mediasoup workers...`)

  for (let i = 0; i < numWorkers; i++) {
    const worker = await mediasoup.createWorker({
      logLevel: mediasoupSettings.worker.logLevel,
      logTags: mediasoupSettings.worker.logTags,
      rtcMinPort: mediasoupSettings.worker.rtcMinPort,
      rtcMaxPort: mediasoupSettings.worker.rtcMaxPort,
    })

    worker.on("died", () => {
      console.error(`mediasoup worker ${i} died, exiting...`)
      setTimeout(() => process.exit(1), 2000)
    })

    workers.push(worker)
    console.log(`mediasoup worker ${i} initialized`)
  }
}

// Get next mediasoup worker (round-robin)
function getMediasoupWorker() {
  const worker = workers[nextMediasoupWorkerIdx]
  nextMediasoupWorkerIdx = (nextMediasoupWorkerIdx + 1) % workers.length
  return worker
}

let nextMediasoupWorkerIdx = 0

// Create a mediasoup router for a call
async function createRouter(callId) {
  const worker = getMediasoupWorker()
  const router = await worker.createRouter({ mediaCodecs: mediasoupSettings.router.mediaCodecs })
  routers.set(callId, router)
  console.log(`Created router for call ${callId}`)
  return router
}

// Create a mediasoup transport
async function createTransport(router, userId, callId) {
  const key = `${userId}_${callId}`

  // Check if transport already exists
  if (transports.has(key)) {
    console.log(`Transport already exists for user ${userId} in call ${callId}`)
    const existingTransport = transports.get(key)
    return {
      id: existingTransport.id,
      iceParameters: existingTransport.iceParameters,
      iceCandidates: existingTransport.iceCandidates,
      dtlsParameters: existingTransport.dtlsParameters,
    }
  }

  const transport = await router.createWebRtcTransport(mediasoupSettings.webRtcTransport)

  // Add connection state tracking
  transport.isConnected = false
  transport.isConnecting = false

  // Store transport
  transports.set(key, transport)

  transport.on("dtlsstatechange", (dtlsState) => {
    if (dtlsState === "closed") {
      console.log(`Transport closed for user ${userId} in call ${callId}`)
      transports.delete(key)
    } else if (dtlsState === "connected") {
      transport.isConnected = true
      transport.isConnecting = false
    }
  })

  transport.on("close", () => {
    console.log(`Transport closed for user ${userId} in call ${callId}`)
    transports.delete(key)
  })

  console.log(`Created transport for user ${userId} in call ${callId}`)

  return {
    id: transport.id,
    iceParameters: transport.iceParameters,
    iceCandidates: transport.iceCandidates,
    dtlsParameters: transport.dtlsParameters,
  }
}

// Clean up resources for a call
function cleanupCall(callId) {
  // Close router which will close all transports, producers, consumers
  const router = routers.get(callId)
  if (router) {
    router.close()
    routers.delete(callId)
  }

  // Clean up maps
  for (const [key, transport] of transports.entries()) {
    if (key.endsWith(`_${callId}`)) {
      transport.close()
      transports.delete(key)
    }
  }

  for (const [key] of producers.entries()) {
    if (key.endsWith(`_${callId}`)) {
      producers.delete(key)
    }
  }

  for (const [key] of consumers.entries()) {
    if (key.endsWith(`_${callId}`)) {
      consumers.delete(key)
    }
  }

  console.log(`Cleaned up resources for call ${callId}`)
}

const broadcastUserList = async () => {
  try {
    const allUsers = await User.find({}).select("name email isOnline lastSeen").lean()

    for (const [socketId, connectedUser] of connectedUsers.entries()) {
      const filteredUsers = allUsers
        .filter((user) => user._id.toString() !== connectedUser._id)
        .map((user) => ({
          _id: user._id.toString(),
          name: user.name,
          email: user.email,
          isOnline: user.isOnline,
          lastSeen: user.lastSeen,
        }))

      io.to(socketId).emit("FE-user-list", filteredUsers)
    }
  } catch (error) {
    console.error("Error broadcasting user list:", error)
  }
}

// broadcast call status changes
const broadcastCallStatus = (callId, status, groupId = null, callType = null) => {
  console.log(`Broadcasting call status: ${callId} -> ${status} (${callType})`)

  if (groupId) {
    // Broadcast to all group members
    io.emit("FE-call-status-changed", { callId, status, groupId, callType })
  } else {
    // Broadcast to call participants
    io.to(`call_${callId}`).emit("FE-call-status-changed", { callId, status, callType })
  }
}

io.on("connection", (socket) => {
  console.log("User connected:", socket.id)

  socket.conn.setMaxListeners(50)

  socket.on("BE-register-user", async ({ name, email }) => {
    try {
      let user = await User.findOne({ email })

      if (!user) {
        user = new User({ name, email, socketId: socket.id, isOnline: true })
        await user.save()
      } else {
        user.socketId = socket.id
        user.isOnline = true
        user.lastSeen = new Date()
        await user.save()
      }

      connectedUsers.set(socket.id, {
        _id: user._id.toString(),
        name: user.name,
        email: user.email,
        socketId: socket.id,
        isOnline: user.isOnline,
      })

      socket.emit("FE-registration-success", {
        user: {
          id: user._id.toString(),
          name: user.name,
          email: user.email,
        },
        message: "Registration successful",
      })

      await broadcastUserList()
    } catch (error) {
      console.error("Registration error:", error)
      socket.emit("FE-registration-error", { message: "Registration failed" })
    }
  })

  // Get users list
  socket.on("BE-get-users", async () => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const allUsers = await User.find({}).select("name email isOnline lastSeen").lean()

      const filteredUsers = allUsers
        .filter((user) => user._id.toString() !== currentUser._id)
        .map((user) => ({
          _id: user._id.toString(),
          name: user.name,
          email: user.email,
          isOnline: user.isOnline,
          lastSeen: user.lastSeen,
        }))

      socket.emit("FE-user-list", filteredUsers)
    } catch (error) {
      console.error("Get users error:", error)
      socket.emit("FE-user-list", [])
    }
  })

  socket.on("BE-start-private-chat", async ({ targetUserId, message }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const roomId = [currentUser._id, targetUserId].sort().join("_")

      const targetUser = await User.findById(targetUserId).select("name email isOnline").lean()
      if (!targetUser) return

      const targetUserFormatted = {
        _id: targetUser._id.toString(),
        name: targetUser.name,
        email: targetUser.email,
        isOnline: targetUser.isOnline,
      }

      const targetUserSocket = Array.from(connectedUsers.values()).find((u) => u._id === targetUserId)

      if (targetUserSocket) {
        socket.join(roomId)
        io.to(targetUserSocket.socketId).emit("FE-private-room-joined", {
          roomId,
          withUser: currentUser,
        })
      } else {
        socket.join(roomId)
      }

      socket.emit("FE-private-room-joined", {
        roomId,
        withUser: targetUserFormatted,
      })
    } catch (error) {
      console.error("Start private chat error:", error)
    }
  })

  socket.on("BE-send-private-message", async ({ roomId, message, type }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const chatMessage = new Chat({
        roomId,
        sender: currentUser._id,
        message,
        type: type || "text",
        timestamp: new Date(),
      })

      await chatMessage.save()
      await chatMessage.populate("sender", "name email")

      io.to(roomId).emit("FE-private-message", {
        roomId,
        sender: chatMessage.sender,
        message: chatMessage.message,
        type: chatMessage.type,
        timestamp: chatMessage.timestamp,
      })
    } catch (error) {
      console.error("Send private message error:", error)
    }
  })

  socket.on("BE-get-chat-history", async ({ roomId }) => {
    try {
      const messages = await Chat.find({ roomId }).populate("sender", "name email").sort({ timestamp: 1 }).limit(500)

      socket.emit("FE-chat-history", { roomId, messages })
    } catch (error) {
      console.error("Get chat history error:", error)
    }
  })

  socket.on("BE-create-group", async ({ name, members }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      console.log("Creating group:", { name, members, creator: currentUser._id })

      const group = new Group({
        name,
        creator: currentUser._id,
        members: [currentUser._id, ...members],
      })

      await group.save()
      await group.populate("creator", "name email")
      await group.populate("members", "name email")

      console.log("Group created successfully:", group)

      socket.emit("FE-group-created", { group })

      const allMemberIds = group.members.map((member) => member._id.toString())

      for (const memberId of allMemberIds) {
        if (memberId === currentUser._id) continue

        const memberSocket = Array.from(connectedUsers.values()).find((user) => user._id === memberId)

        if (memberSocket) {
          console.log(`Sending group update to member: ${memberSocket.name} (${memberSocket.socketId})`)

          const memberGroups = await Group.find({ members: memberId })
            .populate("creator", "name email")
            .populate("members", "name email")

          io.to(memberSocket.socketId).emit("FE-group-list", memberGroups)
          io.to(memberSocket.socketId).emit("FE-group-created", { group })
        }
      }

      console.log("Group creation and real-time updates completed")
    } catch (error) {
      console.error("Create group error:", error)
      socket.emit("FE-error", { message: "Failed to create group: " + error.message })
    }
  })

  socket.on("BE-get-groups", async () => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const groups = await Group.find({ members: currentUser._id })
        .populate("creator", "name email")
        .populate("members", "name email")

      socket.emit("FE-group-list", groups)
    } catch (error) {
      console.error("Get groups error:", error)
    }
  })

  socket.on("BE-join-group", async ({ groupId }) => {
    try {
      const group = await Group.findById(groupId).populate("creator", "name email").populate("members", "name email")

      if (group) {
        socket.join(groupId)
        socket.emit("FE-group-joined", { groupId, group })
      }
    } catch (error) {
      console.error("Join group error:", error)
    }
  })

  socket.on("BE-send-group-message", async ({ groupId, message, type }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const chatMessage = new GroupChat({
        groupId,
        sender: currentUser._id,
        message,
        type: type || "text",
        timestamp: new Date(),
      })

      await chatMessage.save()
      await chatMessage.populate("sender", "name email")

      io.to(groupId).emit("FE-group-message", {
        groupId,
        sender: chatMessage.sender,
        message: chatMessage.message,
        type: chatMessage.type,
        timestamp: chatMessage.timestamp,
      })
    } catch (error) {
      console.error("Send group message error:", error)
    }
  })

  socket.on("BE-get-group-history", async ({ groupId }) => {
    try {
      const messages = await GroupChat.find({ groupId })
        .populate("sender", "name email")
        .sort({ timestamp: 1 })
        .limit(500)

      socket.emit("FE-group-history", { groupId, messages })
    } catch (error) {
      console.error("Get group history error:", error)
    }
  })

  // call functionality
  socket.on("BE-initiate-call", async ({ callId, targetUserId, callType, roomId }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      const targetUserSocket = Array.from(connectedUsers.values()).find((u) => u._id === targetUserId)
      const targetUser = await User.findById(targetUserId).select("name email").lean()

      if (!currentUser || !targetUser) {
        socket.emit("FE-error", { message: "User not found" })
        return
      }

      const existingCall = Array.from(activeCalls.values()).find(
        (call) =>
          call.participants &&
          (call.participants.includes(targetUserId) ||
            call.caller._id === targetUserId ||
            call.receiver?._id === targetUserId),
      )

      if (existingCall) {
        return socket.emit("FE-call-engaged", {
          callId,
          userId: targetUserId,
          userName: targetUser.name,
        })
      }

      const callHistory = new CallHistory({
        callId,
        caller: currentUser._id,
        receiver: targetUserId,
        callType,
        status: "ringing",
        startTime: new Date(),
        isGroupCall: false,
      })
      await callHistory.save()

      activeCalls.set(callId, {
        callId,
        caller: currentUser,
        receiver: {
          _id: targetUser._id.toString(),
          name: targetUser.name,
          email: targetUser.email,
        },
        callType,
        status: "ringing",
        startTime: new Date(),
        isGroupCall: false,
        participants: [currentUser._id, targetUserId],
      })

      if (targetUserSocket) {
        io.to(targetUserSocket.socketId).emit("FE-incoming-call", {
          callId,
          caller: currentUser,
          receiver: {
            _id: targetUser._id.toString(),
            name: targetUser.name,
            email: targetUser.email,
          },
          callType,
          isGroupCall: false,
          status: "ringing",
        })
      }

      console.log(`Call initiated: ${currentUser.name} calling ${targetUser.name}`)
    } catch (error) {
      console.error("Initiate call error:", error)
      socket.emit("FE-error", { message: "Failed to initiate call" })
    }
  })

  socket.on("BE-initiate-group-call", async ({ callId, groupId, callType }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const group = await Group.findById(groupId).populate("members", "name email")
      if (!group) return

      const callHistory = new CallHistory({
        callId,
        caller: currentUser._id,
        groupId,
        callType,
        status: "ringing",
        startTime: new Date(),
        isGroupCall: true,
        participants: [currentUser._id],
      })
      await callHistory.save()

      activeCalls.set(callId, {
        callId,
        caller: currentUser,
        groupId,
        groupName: group.name,
        callType,
        status: "ringing",
        startTime: new Date(),
        isGroupCall: true,
        participants: [currentUser._id],
      })

      // Create a mediasoup router for this group call
      await createRouter(callId)

      socket.join(`call_${callId}`)

      // 40-second timeout
      const timeout = setTimeout(() => {
        const call = activeCalls.get(callId)
        if (call && call.status === "ringing") {
          console.log(`Group call ${callId} timed out after 40 seconds`)

          CallHistory.findOneAndUpdate(
            { callId },
            {
              status: "missed",
              endTime: new Date(),
            },
          ).catch(console.error)

          activeCalls.delete(callId)
          cleanupCall(callId)
          callTimeouts.delete(callId)

          broadcastCallStatus(callId, "ended", groupId, callType)
          io.to(`call_${callId}`).emit("FE-call-ended", { callId, groupId })
        }
      }, 40000)

      callTimeouts.set(callId, timeout)

      // Send ringing notification
      group.members.forEach((member) => {
        const memberUser = Array.from(connectedUsers.values()).find((u) => u._id === member._id.toString())
        if (memberUser && memberUser._id !== currentUser._id) {
          io.to(memberUser.socketId).emit("FE-incoming-call", {
            callId,
            caller: currentUser,
            groupName: group.name,
            groupId: groupId,
            callType,
            isGroupCall: true,
            status: "ringing",
          })
        }
      })

      console.log(`Group call initiated: ${currentUser.name} calling group ${group.name}`)
    } catch (error) {
      console.error("Initiate group call error:", error)
    }
  })

  socket.on("BE-accept-call", async ({ callId, callType }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const call = activeCalls.get(callId)
      if (!call) {
        socket.emit("FE-error", { message: "Call not found" })
        return
      }

      if (callTimeouts.has(callId)) {
        clearTimeout(callTimeouts.get(callId))
        callTimeouts.delete(callId)
      }

      call.status = call.isGroupCall ? "active" : "connected"
      call.acceptTime = new Date()

      if (call.isGroupCall) {
        if (!call.participants) {
          call.participants = [call.caller._id]
        }

        if (!call.participants.includes(currentUser._id)) {
          call.participants.push(currentUser._id)
        }

        activeCalls.set(callId, call)

        socket.join(`call_${callId}`)

        await CallHistory.findOneAndUpdate(
          { callId },
          {
            status: "accepted",
            acceptTime: new Date(),
            $addToSet: { participants: currentUser._id },
          },
        )

        // Notify all participants about the new user joining
        socket.to(`call_${callId}`).emit("FE-user-joined-call", {
          userId: currentUser._id,
          userInfo: {
            id: currentUser._id,
            name: currentUser.name,
            email: currentUser.email,
          },
        })

        // Broadcast that call is now active stops ringing for others
        broadcastCallStatus(callId, "active", call.groupId, call.callType)

        const callerUser = Array.from(connectedUsers.values()).find((u) => u._id === call.caller._id)
        if (callerUser) {
          io.to(callerUser.socketId).emit("FE-call-accepted", {
            callId,
            acceptedBy: currentUser,
            isGroupCall: true,
          })
        }

        console.log(`Group call accepted: ${currentUser.name} joined call ${callId}`)
      } else {
        activeCalls.set(callId, call)

        socket.join(`call_${callId}`)

        await CallHistory.findOneAndUpdate(
          { callId },
          {
            status: "accepted",
            acceptTime: new Date(),
          },
        )

        const callerUser = Array.from(connectedUsers.values()).find((u) => u._id === call.caller._id)
        if (callerUser) {
          io.to(callerUser.socketId).emit("FE-call-accepted", {
            callId,
            acceptedBy: currentUser,
            isGroupCall: false,
          })
        }

        console.log(`Call accepted: ${currentUser.name} accepted call ${callId}`)
      }
    } catch (error) {
      console.error("Accept call error:", error)
    }
  })

  //joining active group calls
  socket.on("BE-join-active-call", async ({ callId, groupId, callType }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const call = activeCalls.get(callId)
      if (!call || call.status !== "active") {
        socket.emit("FE-error", { message: "Call not found or not active" })
        return
      }

      // Add user to call participants
      if (!call.participants.includes(currentUser._id)) {
        call.participants.push(currentUser._id)
      }

      activeCalls.set(callId, call)

      socket.join(`call_${callId}`)

      // Update call history
      await CallHistory.findOneAndUpdate(
        { callId },
        {
          $addToSet: { participants: currentUser._id },
        },
      )

      // Notify all participants about the new user joining
      socket.to(`call_${callId}`).emit("FE-user-joined-call", {
        userId: currentUser._id,
        userInfo: {
          id: currentUser._id,
          name: currentUser.name,
          email: currentUser.email,
        },
      })

      console.log(`User ${currentUser.name} joined active group call ${callId}`)
    } catch (error) {
      console.error("Join active call error:", error)
    }
  })

  socket.on("BE-reject-call", async ({ callId, reason }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const call = activeCalls.get(callId)
      if (!call) return

      await CallHistory.findOneAndUpdate(
        { callId },
        {
          status: reason === "busy" ? "missed" : "rejected",
          endTime: new Date(),
        },
      )

      if (call.isGroupCall) {
        const callerUser = Array.from(connectedUsers.values()).find((u) => u._id === call.caller._id)
        if (callerUser) {
          io.to(callerUser.socketId).emit("FE-call-rejected", {
            callId,
            rejectedBy: currentUser,
            isGroupCall: true,
            reason,
          })
        }
      } else {
        // For one-to-one calls, end the call
        activeCalls.delete(callId)

        if (callTimeouts.has(callId)) {
          clearTimeout(callTimeouts.get(callId))
          callTimeouts.delete(callId)
        }

        const callerUser = Array.from(connectedUsers.values()).find((u) => u._id === call.caller._id)
        if (callerUser) {
          io.to(callerUser.socketId).emit("FE-call-rejected", {
            callId,
            rejectedBy: currentUser,
            isGroupCall: false,
            reason,
          })
        }
      }

      console.log(`Call rejected: ${currentUser.name} rejected call ${callId}`)
    } catch (error) {
      console.error("Reject call error:", error)
    }
  })

  socket.on("BE-end-call", async ({ callId }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const call = activeCalls.get(callId)
      if (!call) return

      // Calculate call duration
      const endTime = new Date()
      const duration = call.acceptTime ? Math.floor((endTime - call.acceptTime) / 1000) : 0

      if (callTimeouts.has(callId)) {
        clearTimeout(callTimeouts.get(callId))
        callTimeouts.delete(callId)
      }

      // Update call history
      await CallHistory.findOneAndUpdate(
        { callId },
        {
          status: call.acceptTime ? "completed" : "missed",
          endTime,
          duration,
        },
      )

      activeCalls.delete(callId)

      // Clean up mediasoup resources for group calls
      if (call.isGroupCall) {
        cleanupCall(callId)
      }

      // Notify all participants
      io.to(`call_${callId}`).emit("FE-call-ended", { callId, groupId: call.groupId })

      // Broadcast call ended status
      if (call.groupId) {
        broadcastCallStatus(callId, "ended", call.groupId, call.callType)
      }

      if (!call.isGroupCall) {
        // For one-to-one calls, notify both parties
        const callerUser = Array.from(connectedUsers.values()).find((u) => u._id === call.caller._id)
        const receiverUser = Array.from(connectedUsers.values()).find((u) => u._id === call.receiver._id)

        if (callerUser) {
          io.to(callerUser.socketId).emit("FE-call-ended", { callId })
        }
        if (receiverUser) {
          io.to(receiverUser.socketId).emit("FE-call-ended", { callId })
        }
      }

      console.log(`Call ended: ${callId}, duration: ${duration}s`)
    } catch (error) {
      console.error("End call error:", error)
    }
  })

  // individual leaving from group call
  socket.on("BE-leave-call", ({ callId, userId }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const call = activeCalls.get(callId)
      if (!call) return

      console.log(`User ${userId} leaving call ${callId}`)

      socket.leave(`call_${callId}`)
      socket.to(`call_${callId}`).emit("FE-user-left-call", { userId })

      // Clean up user's mediasoup resources for group calls
      if (call.isGroupCall) {
        const transportKey = `${userId}_${callId}`
        const transport = transports.get(transportKey)
        if (transport) {
          transport.close()
          transports.delete(transportKey)
        }

        // Clean up producers
        if (producers.has(transportKey)) {
          const userProducers = producers.get(transportKey)
          for (const producer of userProducers.values()) {
            producer.close()
          }
          producers.delete(transportKey)
        }

        // Clean up consumers
        if (consumers.has(transportKey)) {
          const userConsumers = consumers.get(transportKey)
          for (const consumer of userConsumers.values()) {
            consumer.close()
          }
          consumers.delete(transportKey)
        }
      }

      if (call.participants) {
        call.participants = call.participants.filter((id) => id !== userId)

        // If no participants left, end the call completely
        if (call.participants.length === 0) {
          console.log(`No participants left in call ${callId}, ending call`)

          const endTime = new Date()
          const duration = call.acceptTime ? Math.floor((endTime - call.acceptTime) / 1000) : 0

          if (callTimeouts.has(callId)) {
            clearTimeout(callTimeouts.get(callId))
            callTimeouts.delete(callId)
          }

          CallHistory.findOneAndUpdate(
            { callId },
            {
              status: "completed",
              endTime,
              duration,
            },
          ).catch(console.error)

          activeCalls.delete(callId)

          if (call.isGroupCall) {
            cleanupCall(callId)
          }

          // Broadcast call ended status
          if (call.groupId) {
            broadcastCallStatus(callId, "ended", call.groupId, call.callType)
          }

          io.to(`call_${callId}`).emit("FE-call-ended", { callId, groupId: call.groupId })
        } else {
          activeCalls.set(callId, call)
          io.to(`call_${callId}`).emit("FE-user-left-call", { userId: userId })
        }
      }
    } catch (error) {
      console.error("Leave call error:", error)
    }
  })

  // P2P WebRTC handlers (for one-to-one calls)
  socket.on("BE-webrtc-offer", async ({ to, offer, callId }) => {
    try {
      const targetUserSocket = Array.from(connectedUsers.values()).find((u) => u._id === to)
      if (targetUserSocket) {
        io.to(targetUserSocket.socketId).emit("FE-webrtc-offer", {
          from: connectedUsers.get(socket.id)._id,
          offer,
          callId,
        })
      }
    } catch (error) {
      console.error("WebRTC offer error:", error)
    }
  })

  socket.on("BE-webrtc-answer", async ({ to, answer, callId }) => {
    try {
      const targetUserSocket = Array.from(connectedUsers.values()).find((u) => u._id === to)
      if (targetUserSocket) {
        io.to(targetUserSocket.socketId).emit("FE-webrtc-answer", {
          from: connectedUsers.get(socket.id)._id,
          answer,
          callId,
        })
      }
    } catch (error) {
      console.error("WebRTC answer error:", error)
    }
  })

  socket.on("BE-webrtc-ice-candidate", async ({ to, candidate, callId }) => {
    try {
      const targetUserSocket = Array.from(connectedUsers.values()).find((u) => u._id === to)
      if (targetUserSocket) {
        io.to(targetUserSocket.socketId).emit("FE-webrtc-ice-candidate", {
          from: connectedUsers.get(socket.id)._id,
          candidate,
          callId,
        })
      }
    } catch (error) {
      console.error("WebRTC ICE candidate error:", error)
    }
  })

  // mediasoup handlers (for group calls only)
  socket.on("BE-get-router-rtpCapabilities", async ({ callId }) => {
    try {
      const router = routers.get(callId)
      if (!router) {
        socket.emit("FE-error", { message: "Call router not found" })
        return
      }

      socket.emit("FE-router-rtpCapabilities", {
        rtpCapabilities: router.rtpCapabilities,
      })
    } catch (error) {
      console.error("Error getting router RTP capabilities:", error)
      socket.emit("FE-error", { message: "Failed to get router capabilities" })
    }
  })

  socket.on("BE-create-transport", async ({ callId, direction }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const router = routers.get(callId)
      if (!router) {
        socket.emit("FE-error", { message: "Call router not found" })
        return
      }

      const key = `${currentUser._id}_${callId}`

      // Check if transport already exists for this user and call
      if (transports.has(key)) {
        console.log(`Transport already exists for user ${currentUser._id} in call ${callId}`)
        const existingTransport = transports.get(key)

        socket.emit("FE-transport-created", {
          direction,
          transport: {
            id: existingTransport.id,
            iceParameters: existingTransport.iceParameters,
            iceCandidates: existingTransport.iceCandidates,
            dtlsParameters: existingTransport.dtlsParameters,
          },
        })
        return
      }

      const transport = await createTransport(router, currentUser._id, callId)

      socket.emit("FE-transport-created", {
        direction,
        transport,
      })
    } catch (error) {
      console.error("Error creating transport:", error)
      socket.emit("FE-error", { message: "Failed to create transport: " + error.message })
    }
  })

  socket.on("BE-connect-transport", async ({ callId, transportId, dtlsParameters }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const key = `${currentUser._id}_${callId}`
      const transport = transports.get(key)

      if (!transport) {
        socket.emit("FE-error", { message: "Transport not found" })
        return
      }

      // Check if transport is already connected or connecting
      if (transport.isConnected) {
        console.log(`Transport already connected for user ${currentUser._id}`)
        socket.emit("FE-transport-connected", { transportId })
        return
      }

      if (transport.isConnecting) {
        console.log(`Transport already connecting for user ${currentUser._id}`)
        return
      }

      // Mark as connecting to prevent duplicate calls
      transport.isConnecting = true

      try {
        await transport.connect({ dtlsParameters })
        transport.isConnected = true
        transport.isConnecting = false
        console.log(`Transport connected successfully for user ${currentUser._id}`)
        socket.emit("FE-transport-connected", { transportId })
      } catch (error) {
        transport.isConnecting = false
        throw error
      }
    } catch (error) {
      console.error("Error connecting transport:", error)
      socket.emit("FE-error", { message: "Failed to connect transport: " + error.message })
    }
  })

  socket.on("BE-produce", async ({ callId, transportId, kind, rtpParameters, appData }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const key = `${currentUser._id}_${callId}`
      const transport = transports.get(key)

      if (!transport) {
        socket.emit("FE-error", { message: "Transport not found" })
        return
      }

      const producer = await transport.produce({
        kind,
        rtpParameters,
        appData: { ...appData, userId: currentUser._id },
      })

      // Store producer
      if (!producers.has(key)) {
        producers.set(key, new Map())
      }
      producers.get(key).set(kind, producer)

      producer.on("transportclose", () => {
        console.log(`Producer transport closed for user ${currentUser._id}`)
        producer.close()
      })

      // Notify clients about new producer
      socket.to(`call_${callId}`).emit("FE-new-producer", {
        producerId: producer.id,
        userId: currentUser._id,
        kind,
      })

      socket.emit("FE-producer-created", {
        id: producer.id,
      })

      // Notify about track state
      socket.to(`call_${callId}`).emit("FE-track-state-changed", {
        from: currentUser._id,
        trackType: kind,
        enabled: true,
      })
    } catch (error) {
      console.error("Error producing:", error)
      socket.emit("FE-error", { message: "Failed to produce media" })
    }
  })

  socket.on("BE-consume", async ({ callId, producerId, rtpCapabilities }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const router = routers.get(callId)
      if (!router) {
        socket.emit("FE-error", { message: "Call router not found" })
        return
      }

      // Check if consumer can consume this producer
      if (!router.canConsume({ producerId, rtpCapabilities })) {
        socket.emit("FE-error", { message: "Cannot consume this producer" })
        return
      }

      // Find the transport for this user
      const key = `${currentUser._id}_${callId}`
      const transport = transports.get(key)

      if (!transport) {
        socket.emit("FE-error", { message: "Transport not found" })
        return
      }

      // Create consumer
      const consumer = await transport.consume({
        producerId,
        rtpCapabilities,
        paused: true, // Start paused, client will resume
      })

      // Store consumer
      if (!consumers.has(key)) {
        consumers.set(key, new Map())
      }
      consumers.get(key).set(producerId, consumer)

      consumer.on("transportclose", () => {
        console.log(`Consumer transport closed for user ${currentUser._id}`)
        consumer.close()
      })

      // Find producer owner
      let producerUserId = null
      for (const [userKey, userProducers] of producers.entries()) {
        for (const [kind, producer] of userProducers.entries()) {
          if (producer.id === producerId) {
            producerUserId = userKey.split("_")[0]
            break
          }
        }
        if (producerUserId) break
      }

      socket.emit("FE-consumer-created", {
        id: consumer.id,
        producerId,
        kind: consumer.kind,
        rtpParameters: consumer.rtpParameters,
        producerUserId,
      })
    } catch (error) {
      console.error("Error consuming:", error)
      socket.emit("FE-error", { message: "Failed to consume media" })
    }
  })

  socket.on("BE-resume-consumer", async ({ callId, consumerId }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const key = `${currentUser._id}_${callId}`

      if (!consumers.has(key)) {
        socket.emit("FE-error", { message: "No consumers found for this user" })
        return
      }

      // Find the consumer
      let consumer = null
      for (const c of consumers.get(key).values()) {
        if (c.id === consumerId) {
          consumer = c
          break
        }
      }

      if (!consumer) {
        socket.emit("FE-error", { message: "Consumer not found" })
        return
      }

      await consumer.resume()
      socket.emit("FE-consumer-resumed", { consumerId })
    } catch (error) {
      console.error("Error resuming consumer:", error)
      socket.emit("FE-error", { message: "Failed to resume consumer" })
    }
  })

  socket.on("BE-track-state-changed", async ({ callId, trackType, enabled, to }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      // For group calls, notify all participants
      if (callId) {
        socket.to(`call_${callId}`).emit("FE-track-state-changed", {
          from: currentUser._id,
          trackType,
          enabled,
        })
      } else if (to) {
        // For one-to-one calls, notify the specific participant
        const otherUserSocket = Array.from(connectedUsers.values()).find((u) => u._id === to)
        if (otherUserSocket) {
          io.to(otherUserSocket.socketId).emit("FE-track-state-changed", {
            from: currentUser._id,
            trackType,
            enabled,
          })
        }
      }
    } catch (error) {
      console.error("Track state change error:", error)
    }
  })

  // Get call history for specific chat
  socket.on("BE-get-chat-call-history", async ({ roomId, isGroupChat, targetUserId, groupId }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      let callHistory = []

      if (isGroupChat && groupId) {
        // Get group call history
        callHistory = await CallHistory.find({
          groupId: groupId,
          isGroupCall: true,
        })
          .populate("caller", "name email")
          .populate("participants", "name email")
          .sort({ startTime: -1 })
          .limit(20)
      } else if (targetUserId) {
        // Get private call history between current user and target user
        callHistory = await CallHistory.find({
          $and: [
            { isGroupCall: false },
            {
              $or: [
                { caller: currentUser._id, receiver: targetUserId },
                { caller: targetUserId, receiver: currentUser._id },
              ],
            },
          ],
        })
          .populate("caller", "name email")
          .populate("receiver", "name email")
          .sort({ startTime: -1 })
          .limit(20)
      }

      socket.emit("FE-chat-call-history", { roomId, callHistory })
    } catch (error) {
      console.error("Get chat call history error:", error)
    }
  })

  // Get call history
  socket.on("BE-get-call-history", async () => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const callHistory = await CallHistory.find({
        $or: [{ caller: currentUser._id }, { receiver: currentUser._id }, { participants: currentUser._id }],
      })
        .populate("caller", "name email")
        .populate("receiver", "name email")
        .populate("participants", "name email")
        .sort({ startTime: -1 })
        .limit(500)

      socket.emit("FE-call-history", callHistory)
    } catch (error) {
      console.error("Get call history error:", error)
    }
  })

  socket.on("disconnect", async () => {
    console.log("User disconnected:", socket.id)

    const user = connectedUsers.get(socket.id)

    if (user) {
      // Update user offline
      await User.findByIdAndUpdate(user._id, {
        isOnline: false,
        lastSeen: new Date(),
        socketId: null,
      })

      // Handle active calls
      for (const [callId, call] of activeCalls.entries()) {
        if (
          call.caller._id === user._id ||
          call.receiver?._id === user._id ||
          (call.participants && call.participants.includes(user._id))
        ) {
          if (call.isGroupCall && call.participants) {
            call.participants = call.participants.filter((p) => p !== user._id)

            // Clean up user's mediasoup resources
            const transportKey = `${user._id}_${callId}`
            const transport = transports.get(transportKey)
            if (transport) {
              transport.close()
              transports.delete(transportKey)
            }

            // Clean up producers
            if (producers.has(transportKey)) {
              const userProducers = producers.get(transportKey)
              for (const producer of userProducers.values()) {
                producer.close()
              }
              producers.delete(transportKey)
            }

            // Clean up consumers
            if (consumers.has(transportKey)) {
              const userConsumers = consumers.get(transportKey)
              for (const consumer of userConsumers.values()) {
                consumer.close()
              }
              consumers.delete(transportKey)
            }

            if (call.participants.length === 0) {
              if (callTimeouts.has(callId)) {
                clearTimeout(callTimeouts.get(callId))
                callTimeouts.delete(callId)
              }

              await CallHistory.findOneAndUpdate(
                { callId },
                {
                  status: "ended",
                  endTime: new Date(),
                },
              )
              activeCalls.delete(callId)
              cleanupCall(callId)

              if (call.groupId) {
                broadcastCallStatus(callId, "ended", call.groupId, call.callType)
              }

              io.to(`call_${callId}`).emit("FE-call-ended", { callId, groupId: call.groupId })
            } else {
              activeCalls.set(callId, call)
              io.to(`call_${callId}`).emit("FE-user-left-call", { userId: user._id })
            }
          } else {
            // For one-to-one calls, end the call
            if (callTimeouts.has(callId)) {
              clearTimeout(callTimeouts.get(callId))
              callTimeouts.delete(callId)
            }

            await CallHistory.findOneAndUpdate(
              { callId },
              {
                status: "ended",
                endTime: new Date(),
              },
            )
            activeCalls.delete(callId)
            io.to(`call_${callId}`).emit("FE-call-ended", { callId })
          }
        }
      }

      connectedUsers.delete(socket.id)
      await broadcastUserList()
    }
  })
})

const PORT = process.env.PORT || 3001

// Initialize mediasoup workers before starting the server
initializeMediasoupWorkers()
  .then(() => {
    server.listen(PORT, () => {
      dbConnection()
      console.log(`Server running on port ${PORT} with ${workers.length} mediasoup workers`)
    })
  })
  .catch((error) => {
    console.error("Failed to initialize mediasoup workers:", error)
    process.exit(1)
  })
