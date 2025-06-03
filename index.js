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
  pingTimeout: 60000,
  pingInterval: 25000,
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
const callTimeouts = new Map()
// SFU-specific maps
const callHubs = new Map() 
console.log(callHubs,"callHubs");


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

const assignCallHub = (callId, participants) => {
  if (!participants || participants.length === 0) return null

  const callParticipantsList = callParticipants.get(callId)
  let sortedParticipants

  if (callParticipantsList) {
    const participantsWithTime = [...participants].filter(
      (id) => callParticipantsList.has(id) && callParticipantsList.get(id).joinedAt,
    )

    if (participantsWithTime.length > 0) {
      sortedParticipants = participantsWithTime.sort((a, b) => {
        const timeA = callParticipantsList.get(a).joinedAt
        const timeB = callParticipantsList.get(b).joinedAt
        return timeA - timeB
      })
    } else {
      sortedParticipants = [...participants].sort()
    }
  } else {
    sortedParticipants = [...participants].sort()
  }

  const hubUserId = sortedParticipants[0]

  callHubs.set(callId, hubUserId)
  console.log(`Hub assigned for call ${callId}: ${hubUserId}`)

  io.to(`call_${callId}`).emit("FE-hub-assignment", { hubUserId })

  return hubUserId
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

      // Initialize call participants
      callParticipants.set(
        callId,
        new Map([
          [
            currentUser._id,
            {
              userId: currentUser._id,
              userInfo: {
                id: currentUser._id,
                name: currentUser.name,
                email: currentUser.email,
              },
              socketId: socket.id,
              joinedAt: new Date(),
            },
          ],
        ]),
      )

      // Assign the caller as the initial hub for SFU
      assignCallHub(callId, [currentUser._id])

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
          callParticipants.delete(callId)
          callTimeouts.delete(callId)
          callHubs.delete(callId)

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

        if (!callParticipants.has(callId)) {
          callParticipants.set(callId, new Map())
        }

        callParticipants.get(callId).set(currentUser._id, {
          userId: currentUser._id,
          userInfo: {
            id: currentUser._id,
            name: currentUser.name,
            email: currentUser.email,
          },
          socketId: socket.id,
          joinedAt: new Date(),
        })

        socket.join(`call_${callId}`)

        await CallHistory.findOneAndUpdate(
          { callId },
          {
            status: "accepted",
            acceptTime: new Date(),
            $addToSet: { participants: currentUser._id },
          },
        )

        // Check if we need to assign or reassign the hub
        if (!callHubs.has(callId)) {
          assignCallHub(callId, call.participants)
        }

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

  // Hub assignment for SFU architecture
  socket.on("BE-hub-assignment", ({ callId, hubUserId }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      if (!currentUser) return

      const call = activeCalls.get(callId)
      if (!call || !call.isGroupCall) return

      // Only allow hub assignment from participants
      if (!call.participants.includes(currentUser._id)) return

      console.log(`Hub assignment request for call ${callId}: ${hubUserId}`)

      // Update the hub
      callHubs.set(callId, hubUserId)

      // Notify all participants
      io.to(`call_${callId}`).emit("FE-hub-assignment", { hubUserId })

      console.log(`Hub assigned for call ${callId}: ${hubUserId}`)
    } catch (error) {
      console.error("Hub assignment error:", error)
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

      // Add to call participants with detailed info
      if (!callParticipants.has(callId)) {
        callParticipants.set(callId, new Map())
      }

      callParticipants.get(callId).set(currentUser._id, {
        userId: currentUser._id,
        userInfo: {
          id: currentUser._id,
          name: currentUser.name,
          email: currentUser.email,
        },
        socketId: socket.id,
        joinedAt: new Date(),
      })

      socket.join(`call_${callId}`)

      // Update call history
      await CallHistory.findOneAndUpdate(
        { callId },
        {
          $addToSet: { participants: currentUser._id },
        },
      )

      // Send current hub information to the new participant
      if (callHubs.has(callId)) {
        const hubUserId = callHubs.get(callId)
        socket.emit("FE-hub-assignment", { hubUserId })
      }

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
      callParticipants.delete(callId)
      callHubs.delete(callId)

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

      if (callParticipants.has(callId)) {
        callParticipants.get(callId).delete(userId)
        console.log(`Removed ${userId} from call participants. Remaining: ${callParticipants.get(callId).size}`)
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
          callParticipants.delete(callId)
          callHubs.delete(callId)

          // Broadcast call ended status
          if (call.groupId) {
            broadcastCallStatus(callId, "ended", call.groupId, call.callType)
          }

          io.to(`call_${callId}`).emit("FE-call-ended", { callId, groupId: call.groupId })
        } else {
          // Check if the hub left and reassign if needed
          if (callHubs.has(callId) && callHubs.get(callId) === userId) {
            console.log(`Hub ${userId} left call ${callId}, reassigning hub`)

            // Wait a short time to ensure all leave events are processed
            setTimeout(() => {
              assignCallHub(callId, call.participants)
            }, 500)
          }

          activeCalls.set(callId, call)
        }
      }
    } catch (error) {
      console.error("Leave call error:", error)
    }
  })

  // WebRTC signaling for group calls
  socket.on("BE-join-call", ({ callId, userId, userInfo }) => {
    console.log(`User ${userId} joining call ${callId}`, userInfo)

    socket.join(`call_${callId}`)

    const call = activeCalls.get(callId)
    if (!call) {
      console.error(`Call ${callId} not found`)
      return
    }

    if (call.isGroupCall) {
      if (!callParticipants.has(callId)) {
        callParticipants.set(callId, new Map())
      }

      if (!callParticipants.get(callId).has(userId)) {
        callParticipants.get(callId).set(userId, {
          userId,
          userInfo: userInfo || { id: userId, name: "User" },
          socketId: socket.id,
          joinedAt: new Date(),
        })

        console.log(`Added ${userId} to call participants. Total participants: ${callParticipants.get(callId).size}`)
      } else {
        const participant = callParticipants.get(callId).get(userId)
        participant.socketId = socket.id
        participant.rejoinedAt = new Date()
      }

      // Send current hub information to the joining user
      if (callHubs.has(callId)) {
        const hubUserId = callHubs.get(callId)
        socket.emit("FE-hub-assignment", { hubUserId })
      }

      socket.to(`call_${callId}`).emit("FE-user-joined-call", {
        userId,
        userInfo: userInfo || { id: userId, name: "User" },
      })

      if (!call.participants) {
        call.participants = []
      }
      if (!call.participants.includes(userId)) {
        call.participants.push(userId)
        activeCalls.set(callId, call)
      }
    } else {
      // For one-to-one calls, just notify the other participant
      socket.to(`call_${callId}`).emit("FE-user-joined-call", {
        userId,
        userInfo: userInfo || { id: userId, name: "User" },
      })
    }
  })

  // get existing participants for group calls
  socket.on("BE-get-call-participants", ({ callId }) => {
    console.log(`Getting participants for call ${callId}`)

    const participants = callParticipants.get(callId)
    if (participants) {
      const participantsList = Array.from(participants.values()).filter((p) => {
        // Only include participants that are still connected
        const user = Array.from(connectedUsers.values()).find((u) => u._id === p.userId)
        return user && user.socketId
      })

      console.log(
        `Sending ${participantsList.length} existing participants for call ${callId}:`,
        participantsList.map((p) => p.userInfo.name || p.userId),
      )

      socket.emit("FE-existing-participants", {
        participants: participantsList,
      })

      // Also send current hub information
      if (callHubs.has(callId)) {
        const hubUserId = callHubs.get(callId)
        socket.emit("FE-hub-assignment", { hubUserId })
      }
    } else {
      console.log(`No participants found for call ${callId}`)
      socket.emit("FE-existing-participants", { participants: [] })
    }
  })

  socket.on("BE-webrtc-offer", ({ to, offer, callId }) => {
    try {
      const currentUser = connectedUsers.get(socket.id)
      const targetUser = Array.from(connectedUsers.values()).find((u) => u._id === to)

      if (targetUser && currentUser) {
        console.log(`Forwarding WebRTC offer from ${currentUser.name} to ${targetUser.name} for call ${callId}`)
        io.to(targetUser.socketId).emit("FE-webrtc-offer", {
          from: currentUser._id,
          offer,
          callId,
        })
      } else {
        console.error(`Could not find target user ${to} for WebRTC offer`)
      }
    } catch (error) {
      console.error("WebRTC offer forwarding error:", error)
    }
  })

  socket.on("BE-track-state-changed", ({ to, trackType, enabled, callId }) => {
    try {
      console.log(`Track state change: ${trackType} = ${enabled} from ${socket.id} to ${to} for call ${callId}`)

      const currentUser = connectedUsers.get(socket.id)
      const targetUser = Array.from(connectedUsers.values()).find((u) => u._id === to)

      if (targetUser && currentUser) {
        io.to(targetUser.socketId).emit("FE-track-state-changed", {
          from: currentUser._id,
          trackType,
          enabled,
          callId,
        })
      } else {
        console.error(`Could not find target user ${to} for track state change`)
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
        console.log(`Forwarding WebRTC answer from ${currentUser.name} to ${targetUser.name} for call ${callId}`)
        io.to(targetUser.socketId).emit("FE-webrtc-answer", {
          from: currentUser._id,
          answer,
          callId,
        })
      } else {
        console.error(`Could not find target user ${to} for WebRTC answer`)
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
      } else {
        console.error(`Could not find target user ${to} for ICE candidate`)
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

      // active calls
      for (const [callId, call] of activeCalls.entries()) {
        if (
          call.caller._id === user._id ||
          call.receiver?._id === user._id ||
          (call.participants && call.participants.includes(user._id))
        ) {
          if (call.isGroupCall && call.participants) {
            call.participants = call.participants.filter((p) => p !== user._id)

            if (callParticipants.has(callId)) {
              callParticipants.get(callId).delete(user._id)
            }

            // Check if the hub disconnected
            if (callHubs.has(callId) && callHubs.get(callId) === user._id) {
              console.log(`Hub ${user._id} disconnected from call ${callId}, reassigning hub`)
              if (call.participants.length > 0) {
                assignCallHub(callId, call.participants)
              }
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
              callParticipants.delete(callId)
              callHubs.delete(callId)

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
server.listen(PORT, () => {
  dbConnection()
  console.log(`Server running on port ${PORT}`)
})
