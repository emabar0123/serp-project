import express from "express";
import { MongoClient } from "mongodb";

const app = express();
const port = process.env.PORT || 3000;

const mongoUri =
  process.env.MONGO_URL ||
  "mongodb://admin:123@172.16.161.45:27017/?authSource=admin";
const dbName = process.env.MONGO_DB || "serpents-config";
const collectionName = process.env.MONGO_COLLECTION || "configurations";

let client;

async function getCollection() {
  if (!client) {
    client = new MongoClient(mongoUri);
    await client.connect();
  }
  return client.db(dbName).collection(collectionName);
}

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.get("/configurations/by-name/:name", async (req, res) => {
  try {
    const collection = await getCollection();
    const doc = await collection.findOne({
      configuration_name: req.params.name,
    });

    if (!doc) {
      return res.status(404).json({ error: "Configuration not found" });
    }

    return res.json(doc);
  } catch (err) {
    return res.status(500).json({ error: "Server error" });
  }
});

app.listen(port, () => {
  console.log(`Config server listening on port ${port}`);
});
