const express = require("express")
const http = require("http")
const socketIo = require("socket.io")
const cors = require("cors")
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
})

const dbConnection = require("./src/config/Db.config")

const CallHistory = require("./src/models/CallHistory")
const GroupChat = require("./src/models/GroupChat")
const Group = require("./src/models/Group")
const Chat = require("./src/models/Chat")
const User = require("./src/models/User")

const connectedUsers = new Map()
const activeCalls = new Map()
const callParticipants = new Map()
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




io.on("connection", (socket) => {
  console.log("User connected:", socket.id)

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

      const group = new Group({
        name,
        creator: currentUser._id,
        members: [currentUser._id, ...members],
      })

      await group.save()
      await group.populate("creator", "name email")
      await group.populate("members", "name email")

      socket.emit("FE-group-created", { group })
    } catch (error) {
      console.error("Create group error:", error)
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

  // Call functionality
  socket.on("BE-initiate-call", async ({ callId, targetUserId, callType, roomId }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      const targetUserSocket = Array.from(connectedUsers.values()).find((u) => u._id === targetUserId)
      const targetUser = await User.findById(targetUserId).select("name email").lean()

      if (!currentUser || !targetUser) {
        socket.emit("FE-error", { message: "User not found" })
        return
      }

      // Create call history record
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

      // Store active call
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
      })

      // Send call notification to receiver if online
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

      // Create call history record
      const callHistory = new CallHistory({
        callId,
        caller: currentUser._id,
        groupId,
        callType,
        status: "ringing",
        startTime: new Date(),
        isGroupCall: true,
        participants: group.members.map((m) => m._id),
      })
      await callHistory.save()

      // Store active call
      activeCalls.set(callId, {
        callId,
        caller: currentUser,
        groupId,
        groupName: group.name,
        callType,
        status: "ringing",
        startTime: new Date(),
        isGroupCall: true,
        participants: [],
      })

      // Send call notification to all group members except caller
      group.members.forEach((member) => {
        const memberUser = Array.from(connectedUsers.values()).find((u) => u._id === member._id.toString())
        if (memberUser && memberUser._id !== currentUser._id) {
          io.to(memberUser.socketId).emit("FE-incoming-call", {
            callId,
            caller: currentUser,
            groupName: group.name,
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

      // Update call status
      call.status = "connected"
      call.acceptTime = new Date()
      activeCalls.set(callId, call)

      // Update call history
      await CallHistory.findOneAndUpdate(
        { callId },
        {
          status: "accepted",
          acceptTime: new Date(),
          $addToSet: { participants: currentUser._id },
        },
      )

      if (call.isGroupCall) {
        // Add user to call participants
        if (!callParticipants.has(callId)) {
          callParticipants.set(callId, new Set())
        }
        callParticipants.get(callId).add(currentUser._id)

        // Join call room
        socket.join(`call_${callId}`)

        // Notify caller and other participants
        socket.to(`call_${callId}`).emit("FE-user-joined-call", {
          userId: currentUser._id,
          userInfo: currentUser,
        })

        // Notify caller about acceptance
        const callerUser = Array.from(connectedUsers.values()).find((u) => u._id === call.caller._id)
        if (callerUser) {
          io.to(callerUser.socketId).emit("FE-call-accepted", {
            callId,
            acceptedBy: currentUser,
            isGroupCall: true,
          })
        }
      } else {
        // One-to-one call
        socket.join(`call_${callId}`)

        // Notify caller
        const callerUser = Array.from(connectedUsers.values()).find((u) => u._id === call.caller._id)
        if (callerUser) {
          io.to(callerUser.socketId).emit("FE-call-accepted", {
            callId,
            acceptedBy: currentUser,
            isGroupCall: false,
          })
        }
      }

      console.log(`Call accepted: ${currentUser.name} accepted call ${callId}`)
    } catch (error) {
      console.error("Accept call error:", error)
    }
  })

  socket.on("BE-reject-call", async ({ callId }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const call = activeCalls.get(callId)
      if (!call) return

      // Update call history
      await CallHistory.findOneAndUpdate(
        { callId },
        {
          status: "rejected",
          endTime: new Date(),
        },
      )

      if (call.isGroupCall) {
        // For group calls, just notify that this user rejected
        const callerUser = Array.from(connectedUsers.values()).find((u) => u._id === call.caller._id)
        if (callerUser) {
          io.to(callerUser.socketId).emit("FE-call-rejected", {
            callId,
            rejectedBy: currentUser,
            isGroupCall: true,
          })
        }
      } else {
        // For one-to-one calls, end the call
        activeCalls.delete(callId)

        // Notify caller
        const callerUser = Array.from(connectedUsers.values()).find((u) => u._id === call.caller._id)
        if (callerUser) {
          io.to(callerUser.socketId).emit("FE-call-rejected", {
            callId,
            rejectedBy: currentUser,
            isGroupCall: false,
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

      // Update call history
      await CallHistory.findOneAndUpdate(
        { callId },
        {
          status: call.acceptTime ? "completed" : "missed",
          endTime,
          duration,
        },
      )

      // Clean up
      activeCalls.delete(callId)
      callParticipants.delete(callId)

      // Notify all participants
      io.to(`call_${callId}`).emit("FE-call-ended", { callId })

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

  // WebRTC signaling
  socket.on("BE-join-call", ({ callId, userId, userInfo }) => {
    socket.join(`call_${callId}`)
    socket.to(`call_${callId}`).emit("FE-user-joined-call", { userId, userInfo })
  })

  socket.on("BE-leave-call", ({ callId, userId }) => {
    socket.leave(`call_${callId}`)
    socket.to(`call_${callId}`).emit("FE-user-left-call", { userId })
  })
socket.on("BE-webrtc-offer", ({ to, offer, callId }) => {
  try {
    const currentUser = connectedUsers.get(socket.id)
    const targetUser = Array.from(connectedUsers.values()).find((u) => u._id === to)

    if (targetUser && currentUser) {
      console.log(`Forwarding WebRTC offer from ${currentUser.name} to ${targetUser.name}`)
      io.to(targetUser.socketId).emit("FE-webrtc-offer", {
        from: currentUser._id,
        offer,
        callId,
      })
    }
  } catch (error) {
    console.error("WebRTC offer forwarding error:", error)
  }
})

  socket.on("BE-track-state-changed", ({ to, trackType, enabled, callId }) => {
  try {
    console.log(`Track state change: ${trackType} = ${enabled} from ${socket.id} to ${to}`)

    const targetUser = Array.from(connectedUsers.values()).find((u) => u._id === to)
    if (targetUser) {
      io.to(targetUser.socketId).emit("FE-track-state-changed", {
        from: connectedUsers.get(socket.id)?._id,
        trackType,
        enabled,
        callId,
      })
    }
  } catch (error) {
    console.error("Track state change error:", error)
  }
})

socket.on("BE-webrtc-answer", ({ to, answer, callId }) => {
  try {
    const currentUser = connectedUsers.get(socket.id)
    const targetUser = Array.from(connectedUsers.values()).find((u) => u._id === to)

    if (targetUser && currentUser) {
      console.log(`Forwarding WebRTC answer from ${currentUser.name} to ${targetUser.name}`)
      io.to(targetUser.socketId).emit("FE-webrtc-answer", {
        from: currentUser._id,
        answer,
        callId,
      })
    }
  } catch (error) {
    console.error("WebRTC answer forwarding error:", error)
  }
})
  socket.on("BE-webrtc-ice-candidate", ({ to, candidate, callId }) => {
  try {
    const currentUser = connectedUsers.get(socket.id)
    const targetUser = Array.from(connectedUsers.values()).find((u) => u._id === to)

    if (targetUser && currentUser) {
      io.to(targetUser.socketId).emit("FE-webrtc-ice-candidate", {
        from: currentUser._id,
        candidate,
        callId,
      })
    }
  } catch (error) {
    console.error("ICE candidate forwarding error:", error)
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

  // Handle disconnect
  socket.on("disconnect", async () => {
    console.log("User disconnected:", socket.id)

    const user = connectedUsers.get(socket.id)
    if (user) {
      // Update user status to offline
      await User.findByIdAndUpdate(user._id, {
        isOnline: false,
        lastSeen: new Date(),
        socketId: null,
      })

      // End any active calls
      for (const [callId, call] of activeCalls.entries()) {
        if (call.caller._id === user._id || call.receiver?._id === user._id) {
          // End the call
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

      connectedUsers.delete(socket.id)

      await broadcastUserList()
    }
  })
})

const PORT = process.env.PORT || 3001
server.listen(PORT, () => {
  dbConnection();
  console.log(`Server running on port ${PORT}`)
})
