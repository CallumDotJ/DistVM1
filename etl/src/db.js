const mysql = require("mysql2/promise");

function makePool() {
  return mysql.createPool({
    host: process.env.MYSQL_SERVICE || "mysql",
    user: process.env.MYSQL_USER || "admin",
    password: process.env.MYSQL_PASSWORD || "admin",
    database: process.env.MYSQL_DATABASE || "jokes_db",
    port: Number(process.env.MYSQL_PORT_INTERNAL || 3306),
    waitForConnections: true,
    connectionLimit: 10
  });
}

module.exports = { makePool };