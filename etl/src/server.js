const express = require("express");
const amqp = require("amqplib");
const { makePool } = require("./db");

const app = express();
const PORT = process.env.PORT || 3000;

// RABBITMQ Confiq
const RMQ_HOST = process.env.RABBITMQ_HOST || "rabbitmq";
const RMQ_PORT = process.env.RABBITMQ_PORT || 5672;
const RMQ_USER = process.env.RABBITMQ_DEFAULT_USER || "admin";
const RMQ_PASS = process.env.RABBITMQ_DEFAULT_PASS || "admin";
const QUEUE_NAME = process.env.QUEUE_NAME || "submit_queue";

// RabbitMQ constr
const AMQP_URL =
  process.env.AMQP_URL ||
  `amqp://${RMQ_USER}:${RMQ_PASS}@${RMQ_HOST}:${RMQ_PORT}`;

// RabbitMQ queues and exchange
const MODERATED_QUEUE_NAME =
  process.env.MODERATED_QUEUE_NAME || "moderated_queue";
const EXCHANGE = "type_update_exchange";

//db
const pool = makePool();

app.get("/alive", (req, res) => {
  res.send({ ok: "Alive", service: "ETL" });
});

//Function to process the message and send to the DB.
async function processMessage(msgObj) {
  
  // transform step - normalise type and trim strings
  const setup = String(msgObj.setup || "").trim();
  const punchline = String(msgObj.punchline || "").trim();
  const type = String(msgObj.type || "")
    .trim()
    .toLowerCase();

  if (!setup || !punchline || !type) {
    throw new Error("Invalid payload: setup/punchline/type required");
  }

  const conn = await pool.getConnection();
  try {

    await conn.beginTransaction();

    // load 1 - insert type if new - sql stop duplicate
    const [newTypeResult] = await conn.execute(
      "INSERT IGNORE INTO tbl_type (type) VALUES (?)",
      [type],
    );

    const isNewType = newTypeResult.affectedRows === 1; // if 0 then itll false

    // load step 2 - lookup type id
    const [rows] = await conn.execute(
      "SELECT id FROM tbl_type WHERE type = ? LIMIT 1",
      [type],
    );

    // for whatever reason its not in the db = error
    const typeId = rows?.[0]?.id;
    if (!typeId) {console.log("no match for typeID")};

    // load step 3 - insert joke
    await conn.execute(
      "INSERT INTO tbl_jokes (setup, punchline, type) VALUES (?, ?, ?)",
      [setup, punchline, typeId],
    );

    // get all types to send to exchange

    let allTypes = null;

    if(isNewType)
    {
      allTypes = await getAllTypes(conn);
    }

    await conn.commit(); // commit transaction

    return { isNewType, type, allTypes };
  } catch (e) {
    await conn.rollback(); // rollback on error
    throw e;
  } finally {
    conn.release(); // release connection back to pool
  }
}

async function startConsumer() {
  // connect to RabbitMQ and start consuming messages
  const conn = await amqp.connect(AMQP_URL);
  const channel = await conn.createChannel();

  // submit queue assert = cons
  await channel.assertQueue(QUEUE_NAME, { durable: true });

  // moderated queue assert = prod
  await channel.assertQueue(MODERATED_QUEUE_NAME, { durable: true });

  // type_update event subs
  await channel.assertExchange(EXCHANGE, "fanout", { durable: true });
  console.log("ETL asserted exchange  CRAETED WOOOOO")

  channel.prefetch(1); // process one message at a time

  console.log(`ETL consuming queue: ${QUEUE_NAME}`);
  
  // NEW MODERATOR QUEUE CONSUMPTION
  channel.consume(QUEUE_NAME, async (msg) => {
    if (!msg) return;

    try {
      const msgObj = JSON.parse(msg.content.toString());
      const result = await processMessage(msgObj); //

      if (result.isNewType === true) {
        // publish logic
        await publishNewType(channel, result.allTypes);
      }

      // acknowledge message only after successful processing
      channel.ack(msg);
      console.log(" ETL wrote message to DB:", msgObj.type);
    } catch (err) {
      console.error(" ETL failed:", err.message);
      // If DB down temporarily, requeue so it can be retried
      channel.nack(msg, false, true);
    }
  });
}

// function to send new type update to the exchange
async function publishNewType(channel, types) {
  // tmp obj
  const eventObj = {
    event: "type_update",
    types,
  };

  // load into msg
  const msg = Buffer.from(JSON.stringify(eventObj));

  channel.publish(
    EXCHANGE,
    "",
    msg
  );
  console.log(`ETL published type_update for "${types.length}"`);
}

// continously tries to start consumer 
async function startConsumerWithRetry() {
  while (true) {
    try {
      await startConsumer();
      break;
    } catch (e) {
      console.error("Rabbut not ready, retrying in 5s:", e.message);
      await new Promise((resolve) => setTimeout(resolve, 5000)); // use promise as wait
    }
  }
}

// returns all types from db 
async function getAllTypes(conn)
{
  const [rows] = await conn.execute(
    "SELECT type FROM tbl_type ORDER BY type ASC"
  );

  return rows.map(r => r.type);
}

startConsumerWithRetry();

app.listen(PORT, "0.0.0.0", () => {
  console.log(`ETL alive on port ${PORT}`);
});
