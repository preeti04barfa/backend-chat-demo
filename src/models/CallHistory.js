const mongoose = require("mongoose")

const callHistorySchema = new mongoose.Schema(
  {
    callId: {
      type: String,
      required: true,
      unique: true,
      index: true,
    },
    caller: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
      required: true,
    },
    receiver: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
    },
    groupId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Group",
    },
    participants: [
      {
        type: mongoose.Schema.Types.ObjectId,
        ref: "User",
      },
    ],
    callType: {
      type: String,
      enum: ["audio", "video"],
      required: true,
    },
    status: {
      type: String,
      enum: ["ringing", "accepted", "rejected", "missed", "completed", "ended"],
      default: "ringing",
    },
    isGroupCall: {
      type: Boolean,
      default: false,
    },
    startTime: {
      type: Date,
      required: true,
    },
    acceptTime: {
      type: Date,
    },
    endTime: {
      type: Date,
    },
    duration: {
      type: Number, 
      default: 0,
    },
  },
  {
    timestamps: true,
  },
)

module.exports = mongoose.model("CallHistory", callHistorySchema)
