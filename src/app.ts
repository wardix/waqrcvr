import express, { Request, Response, NextFunction } from "express";
import amqp, { Connection, Channel } from "amqplib";
import dotenv from "dotenv";
import winston from "winston";
import moment from "moment-timezone";

const RETRY_DELAY_MS = 1000;

dotenv.config();
const systemTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;

const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp({
      format: () => {
        return moment().tz(systemTimeZone).format("YYYY-MM-DD HH:mm:ss");
      },
    }),
    winston.format.prettyPrint()
  ),
  defaultMeta: { service: process.env.SERVICE_NAME! },
  transports: [new winston.transports.Console()],
});

let connection: Connection | null = null;
let channel: Channel | null = null;

const port = process.env.PORT || 3000;
const app = express();

setupAmqp();

app.use(express.json());

app.get("/", (_req: Request, res: Response) => {
  res.send("OK");
});

app.post("/v1/jobs", authenticateApiKey, async (req: Request, res: Response) => {
  try {
    const job = req.body;
    logger.info("Received job:", job);
    await publish(Buffer.from(JSON.stringify(job)));
    res.status(200).send("Job queued successfully.");
  } catch (error: any) {
    logger.error("Error publishing job:", error.message);
    res.status(500).send("Failed to queue job.");
  }
});

app.listen(port, () => {
  logger.info(`Server running at http://localhost:${port}`);
});

function authenticateApiKey(req: Request, res: Response, next: NextFunction) {
  const apiKey = req.headers["x-api-key"];
  if (apiKey && apiKey === process.env.API_KEY) {
    next();
    return;
  }
  logger.warn("Unauthorized access attempt detected.");
  res.status(401).json({ message: "Not authorized." });
}

async function setupAmqp(): Promise<void> {
  try {
    connection = await amqp.connect(process.env.AMQP_URL!);
    setupConnectionHandlers();
    channel = await connection.createChannel();
    setupAMQPResources();
    logger.info("AMQP setup complete");
  } catch (error: any) {
    logger.error(`AMQP setup error: ${error.message}`);
    retrySetupAmqp();
  }
}

function setupConnectionHandlers() {
  if (!connection) return;

  connection.on("error", (error: Error) => {
    logger.error(`AMQP connection error: ${error.message}`);
    retrySetupAmqp();
  });

  connection.on("close", () => {
    logger.warn("AMQP connection closed");
    retrySetupAmqp();
  });
}

async function setupAMQPResources() {
  if (!channel) return;

  await channel.assertExchange(process.env.JOB_EXCHANGE!, "direct");
  await channel.assertQueue(process.env.JOB_QUEUE!, { durable: true });
  await channel.bindQueue(
    process.env.JOB_QUEUE!,
    process.env.JOB_EXCHANGE!,
    process.env.JOB_ROUTING_KEY!
  );
}

function retrySetupAmqp() {
  logger.warn(`Retrying AMQP setup in ${RETRY_DELAY_MS}ms...`);
  setTimeout(setupAmqp, RETRY_DELAY_MS);
}

async function publish(message: Buffer): Promise<void> {
  if (!channel) {
    throw new Error("Channel is not set up");
  }

  try {
    channel.publish(
      process.env.JOB_EXCHANGE!,
      process.env.JOB_ROUTING_KEY!,
      message,
      {
        persistent: true,
      }
    );
  } catch (error: any) {
    logger.error(`AMQP publish error: ${error.message}`);
    retryPublish(message);
  }
}

function retryPublish(message: Buffer) {
  logger.warn(`Retrying message publish in ${RETRY_DELAY_MS}ms...`);
  setTimeout(() => publish(message), RETRY_DELAY_MS);
}
