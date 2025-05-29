var mongoose = require('mongoose');

function dbConnection() {
  mongoose.connect(process.env.MONGO_URL)
    .then(function() {
      console.log("Database connected successfully");
    })
    .catch(function(error) {
      console.error("Database connection error:", error.message);
    });
}

module.exports = dbConnection;