const express = require("express")
const http = require("http")
const socketIo = require("socket.io")
const cors = require("cors")
require("dotenv").config()

const app = express()
const server = http.createServer(app)

app.use(cors({ origin: "*", methods: ["GET", "POST"], credentials: true }))
app.use(express.json())

const io = socketIo(server, {
  cors: { origin: "*", methods: ["GET", "POST"], credentials: true },
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
const jitsiRooms = new Map() // callId -> roomName

// Call participants tracking
const callParticipants = new Map() // callId -> Set of userIds

// Generate unique Jitsi room name
function generateJitsiRoomName(callId, groupName) {
  const timestamp = Date.now()
  const randomId = Math.random().toString(36).substring(2, 8)
  const sanitizedGroupName = groupName ? groupName.replace(/[^a-zA-Z0-9]/g, "") : "GroupCall"
  return `${sanitizedGroupName}_${callId}_${timestamp}_${randomId}`
}

// Get participants list for a call
function getCallParticipantsList(callId) {
  const participantIds = callParticipants.get(callId) || new Set()
  const participantsList = []

  for (const userId of participantIds) {
    const userInfo = Array.from(connectedUsers.values()).find((u) => u._id === userId)
    if (userInfo) {
      participantsList.push({
        _id: userInfo._id,
        id: userInfo._id,
        name: userInfo.name,
        email: userInfo.email,
      })
    }
  }

  return participantsList
}

// Add participant to call
function addParticipantToCall(callId, userId) {
  if (!callParticipants.has(callId)) {
    callParticipants.set(callId, new Set())
  }
  callParticipants.get(callId).add(userId)
  console.log(`Added participant ${userId} to call ${callId}. Total: ${callParticipants.get(callId).size}`)
}

// Remove participant from call
function removeParticipantFromCall(callId, userId) {
  if (callParticipants.has(callId)) {
    callParticipants.get(callId).delete(userId)
    console.log(`Removed participant ${userId} from call ${callId}. Total: ${callParticipants.get(callId).size}`)

    if (callParticipants.get(callId).size === 0) {
      callParticipants.delete(callId)
      jitsiRooms.delete(callId) // Clean up Jitsi room
      console.log(`No participants left in call ${callId}`)
    }
  }
}

// Broadcast participants update
function broadcastParticipantsUpdate(callId) {
  const participantsList = getCallParticipantsList(callId)
  console.log(
    `Broadcasting participants update for call ${callId}:`,
    participantsList.map((p) => p.name),
  )

  const participantIds = callParticipants.get(callId) || new Set()
  for (const userId of participantIds) {
    const userSocket = Array.from(connectedUsers.values()).find((u) => u._id === userId)
    if (userSocket) {
      io.to(userSocket.socketId).emit("FE-call-participants", {
        participants: participantsList.filter((p) => p._id !== userId),
      })
    }
  }
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

const broadcastCallStatus = (callId, status, groupId = null, callType = null) => {
  console.log(`Broadcasting call status: ${callId} -> ${status} (${callType})`)
  if (groupId) {
    io.emit("FE-call-status-changed", { callId, status, groupId, callType })
  } else {
    io.to(`call_${callId}`).emit("FE-call-status-changed", { callId, status, callType })
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
        isOnline: user.isOnline,
      })

      socket.emit("FE-registration-success", {
        user: { id: user._id.toString(), name: user.name, email: user.email },
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

      // Create Jitsi room for group call
      const jitsiRoomName = generateJitsiRoomName(callId, group.name)
      jitsiRooms.set(callId, jitsiRoomName)

      const callHistory = new CallHistory({
        callId,
        caller: currentUser._id,
        groupId,
        callType,
        status: "ringing",
        startTime: new Date(),
        isGroupCall: true,
        participants: [currentUser._id],
        jitsiRoomName, // Store Jitsi room name
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
        jitsiRoomName,
      })

      addParticipantToCall(callId, currentUser._id)
      socket.join(`call_${callId}`)

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
          jitsiRooms.delete(callId)
          callTimeouts.delete(callId)

          broadcastCallStatus(callId, "ended", groupId, callType)
          io.to(`call_${callId}`).emit("FE-call-ended", { callId, groupId })
        }
      }, 40000)

      callTimeouts.set(callId, timeout)

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

  // Jitsi room creation
  socket.on("BE-create-jitsi-room", ({ callId }) => {
    try {
      const call = activeCalls.get(callId)
      if (!call || !call.isGroupCall) {
        socket.emit("FE-error", { message: "Call not found or not a group call" })
        return
      }

      const roomName = jitsiRooms.get(callId)
      if (roomName) {
        socket.emit("FE-jitsi-room-created", { roomName })
        console.log(`Jitsi room sent to client: ${roomName}`)
      } else {
        socket.emit("FE-error", { message: "Jitsi room not found" })
      }
    } catch (error) {
      console.error("Create Jitsi room error:", error)
      socket.emit("FE-error", { message: "Failed to create Jitsi room" })
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

        addParticipantToCall(callId, currentUser._id)
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

        socket.to(`call_${callId}`).emit("FE-user-joined-call", {
          userId: currentUser._id,
          userInfo: {
            _id: currentUser._id,
            id: currentUser._id,
            name: currentUser.name,
            email: currentUser.email,
          },
          allParticipants: getCallParticipantsList(callId),
        })

        broadcastParticipantsUpdate(callId)
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

  socket.on("BE-join-active-call", async ({ callId, groupId, callType }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const call = activeCalls.get(callId)
      if (!call || call.status !== "active") {
        socket.emit("FE-error", { message: "Call not found or not active" })
        return
      }

      if (!call.participants.includes(currentUser._id)) {
        call.participants.push(currentUser._id)
      }

      addParticipantToCall(callId, currentUser._id)
      activeCalls.set(callId, call)
      socket.join(`call_${callId}`)

      await CallHistory.findOneAndUpdate(
        { callId },
        {
          $addToSet: { participants: currentUser._id },
        },
      )

      socket.to(`call_${callId}`).emit("FE-user-joined-call", {
        userId: currentUser._id,
        userInfo: {
          _id: currentUser._id,
          id: currentUser._id,
          name: currentUser.name,
          email: currentUser.email,
        },
        allParticipants: getCallParticipantsList(callId),
      })

      broadcastParticipantsUpdate(callId)

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

      const endTime = new Date()
      const duration = call.acceptTime ? Math.floor((endTime - call.acceptTime) / 1000) : 0

      if (callTimeouts.has(callId)) {
        clearTimeout(callTimeouts.get(callId))
        callTimeouts.delete(callId)
      }

      await CallHistory.findOneAndUpdate(
        { callId },
        {
          status: call.acceptTime ? "completed" : "missed",
          endTime,
          duration,
        },
      )

      activeCalls.delete(callId)
      jitsiRooms.delete(callId) // Clean up Jitsi room

      io.to(`call_${callId}`).emit("FE-call-ended", { callId, groupId: call.groupId })

      if (call.groupId) {
        broadcastCallStatus(callId, "ended", call.groupId, call.callType)
      }

      if (!call.isGroupCall) {
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

  socket.on("BE-leave-call", ({ callId, userId }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const call = activeCalls.get(callId)
      if (!call) return

      console.log(`User ${userId} leaving call ${callId}`)

      socket.leave(`call_${callId}`)
      removeParticipantFromCall(callId, userId)

      socket.to(`call_${callId}`).emit("FE-user-left-call", {
        userId,
        allParticipants: getCallParticipantsList(callId),
      })

      broadcastParticipantsUpdate(callId)

      if (call.participants) {
        call.participants = call.participants.filter((id) => id !== userId)

        if (
          call.participants.length === 0 ||
          (callParticipants.get(callId) && callParticipants.get(callId).size === 0)
        ) {
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
          jitsiRooms.delete(callId) // Clean up Jitsi room

          if (call.groupId) {
            broadcastCallStatus(callId, "ended", call.groupId, call.callType)
          }

          io.to(`call_${callId}`).emit("FE-call-ended", { callId, groupId: call.groupId })
        } else {
          activeCalls.set(callId, call)
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

  socket.on("BE-get-call-participants", ({ callId }) => {
    try {
      const participantsList = getCallParticipantsList(callId)
      const currentUser = connectedUsers.get(socket.id)

      socket.emit("FE-call-participants", {
        participants: participantsList.filter((p) => p._id !== currentUser._id),
      })
    } catch (error) {
      console.error("Error getting call participants:", error)
    }
  })

  socket.on("BE-track-state-changed", async ({ callId, trackType, enabled, to }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      if (callId) {
        socket.to(`call_${callId}`).emit("FE-track-state-changed", {
          from: currentUser._id,
          trackType,
          enabled,
        })
      } else if (to) {
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

  socket.on("disconnect", async () => {
    console.log("User disconnected:", socket.id)

    const user = connectedUsers.get(socket.id)

    if (user) {
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
            removeParticipantFromCall(callId, user._id)

            io.to(`call_${callId}`).emit("FE-user-left-call", {
              userId: user._id,
              allParticipants: getCallParticipantsList(callId),
            })

            broadcastParticipantsUpdate(callId)

            if (
              call.participants.length === 0 ||
              (callParticipants.get(callId) && callParticipants.get(callId).size === 0)
            ) {
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
              jitsiRooms.delete(callId)

              if (call.groupId) {
                broadcastCallStatus(callId, "ended", call.groupId, call.callType)
              }

              io.to(`call_${callId}`).emit("FE-call-ended", { callId, groupId: call.groupId })
            } else {
              activeCalls.set(callId, call)
            }
          } else {
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

server.listen(PORT, () => {
  dbConnection()
  console.log(`Server running on port ${PORT} with Jitsi Meet integration`)
  console.log(`Group calls will use Jitsi Meet, 1-on-1 calls use P2P WebRTC`)
})