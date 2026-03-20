const express = require("express");
const amqp = require("amqplib");
const { makePool } = require("./db");

const app = express();
const PORT = process.env.PORT || 3000;

const RMQ_HOST = process.env.RABBITMQ_HOST || "rabbitmq";
const RMQ_PORT = process.env.RABBITMQ_PORT || 5672;
const RMQ_USER = process.env.RABBITMQ_DEFAULT_USER || "admin";
const RMQ_PASS = process.env.RABBITMQ_DEFAULT_PASS || "admin";
const QUEUE_NAME = process.env.QUEUE_NAME || "submit_queue";
const RMQ_VM2_PRIVATEIP = process.env.RABBITMQ_VM2_PRIVATEIP || "172.17.0.2"; // for testing with VM2

const AMQP_URL =
  process.env.AMQP_URL ||
  `amqp://${RMQ_USER}:${RMQ_PASS}@${RMQ_VM2_PRIVATEIP}:${RMQ_PORT}`;

const MODERATED_QUEUE_NAME =
  process.env.MODERATED_QUEUE_NAME || "moderated_queue";
const EXCHANGE = "type_update_exchange";

const pool = makePool();

app.get("/alive", (req, res) => {
  res.send({ ok: "Alive", service: "ETL" });
});

/*Function to process the message and send to the DB.*/
async function processMessage(msgObj) {
  // etl function to process each message

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

    // load step 1 - insert type if new (no duplicates thanks to UNIQUE + IGNORE)
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

    const typeId = rows?.[0]?.id;
    if (!typeId) throw new Error("Failed to resolve type id");

    // load step 3 - insert joke
    await conn.execute(
      "INSERT INTO tbl_jokes (setup, punchline, type) VALUES (?, ?, ?)",
      [setup, punchline, typeId],
    );

    await conn.commit(); // commit transaction

    return { isNewType, type };
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

  await channel.assertQueue(QUEUE_NAME, { durable: true });

  await channel.assertQueue(MODERATED_QUEUE_NAME, { durable: true });

  await channel.assertExchange(EXCHANGE, "fanout", { durable: true });

  channel.prefetch(1); // process one message at a time for better reliability

  console.log(`ETL consuming queue: ${QUEUE_NAME}`);
  /* 
  channel.consume(QUEUE_NAME, async (msg) => {
    // callback for each message

    if (!msg) return;

    try {
      const msgObj = JSON.parse(msg.content.toString());
      await processMessage(msgObj); //

      // acknowledge message only after successful processing to avoid data loss
      channel.ack(msg);
      console.log(" ETL wrote message to DB:", msgObj.type);
    } catch (err) {
      console.error(" ETL failed:", err.message);
      // If DB down temporarily, requeue so it can be retried
      channel.nack(msg, false, true);
    }
  }); */

  // NEW MODERATOR QUEUE CONSUMPTION
  //channel.consume(MODERATED_QUEUE_NAME, async (msg) => {
  channel.consume(QUEUE_NAME, async (msg) => {
    if (!msg) return;

    try {
      const msgObj = JSON.parse(msg.content.toString());
      const result = await processMessage(msgObj); //

      if (result.isNewType === true) {
        // publish logic
        await publishNewType(channel, result.type);
      }

      // acknowledge message only after successful processing to avoid data loss
      channel.ack(msg);
      console.log(" ETL wrote message to DB:", msgObj.type);
    } catch (err) {
      console.error(" ETL failed:", err.message);
      // If DB down temporarily, requeue so it can be retried
      channel.nack(msg, false, true);
    }
  });
}

async function publishNewType(channel, type) {
  const eventObj = {
    event: "type_update",
    type,
  };

  const msg = Buffer.from(JSON.stringify(eventObj));

  channel.publish(
    EXCHANGE,
    "",
    msg
  );
  console.log(`ETL published type_update for "${type}"`);
}

async function startConsumerWithRetry() {
  while (true) {
    try {
      await startConsumer();
      break;
    } catch (e) {
      console.error("Rabbut not ready, retrying in 5s:", e.message);
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
}

startConsumerWithRetry();

app.listen(PORT, "0.0.0.0", () => {
  console.log(`ETL alive on port ${PORT}`);
});
